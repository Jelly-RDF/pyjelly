from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from itertools import cycle
from typing import Any, ClassVar, final

from pyjelly._pb2 import rdf_pb2 as pb
from pyjelly._serializing import lookups, streams


class Statement:
    """
    Helper to manage RDF term encoding state during serialization.

    Collects RDF terms and associated rows before constructing a protobuf
    representation. Supports cycling through terms (s/p/o) and tracking
    repeated terms to optimize serialization.
    """

    _preceding_rows: list[pb.RdfStreamRow]
    _term_rows: Sequence[pb.RdfStreamRow]

    current_term: str

    def __init__(self, terms: Sequence[str]) -> None:
        self._preceding_rows = []
        self._term_rows = ()

        self.all_terms = cycle(terms)
        self.current_term = next(self.all_terms)

        self.values: dict[str, Any] = {}

    def _cycle(self) -> None:
        """
        Advance to the next RDF term in the encoding cycle.

        Moves current term's rows to preceding, clears buffers,
        and updates internal term pointer.
        """
        self._preceding_rows.extend(self._term_rows)
        self._term_rows = ()
        self.current_term = next(self.all_terms)

    def _collect(self) -> tuple[tuple[pb.RdfStreamRow, ...], dict[str, Any]]:
        """
        Finalize term collection and return all buffered data.

        Returns
        -------
        tuple of rows and serialized RDF term data.
        """
        term_values = self.values.copy()
        self.values.clear()
        preceding_rows = (*self._preceding_rows, *self._term_rows)
        self._preceding_rows.clear()
        self._term_rows = ()
        return preceding_rows, term_values

    def _get_key(self, term: str) -> str:
        return f"{self.current_term}_{term}"

    def _set_term(
        self,
        term: str,
        value: Any,
        rows: Sequence[pb.RdfStreamRow] = (),
    ) -> None:
        """
        Register a term value and its related rows for the current RDF position.

        Parameters
        ----------
        term
            Logical RDF value kind (e.g. 'iri', 'literal').
        value
            Protobuf message or string.
        rows
            Additional stream rows required for encoding.
        """
        self._term_rows = rows
        self.values[self._get_key(term)] = value

    def add_rows(self, *rows: pb.RdfStreamRow) -> None:
        """
        Add supporting rows preceding the current RDF term.

        Parameters
        ----------
        rows
            Stream rows to prepend before current term data.
        """
        self._preceding_rows.extend(rows)

    def set_iri(self, value: Any, rows: Sequence[pb.RdfStreamRow] = ()) -> None:
        self._set_term("iri", value, rows)

    def set_bnode(self, value: Any, rows: Sequence[pb.RdfStreamRow] = ()) -> None:
        self._set_term("bnode", value, rows)

    def set_literal(self, value: Any, rows: Sequence[pb.RdfStreamRow] = ()) -> None:
        self._set_term("literal", value, rows)


class StatementEncoder(metaclass=ABCMeta):
    """
    Base class for Jelly statement encoders.

    Provides lookup logic, repeated term optimization,
    and encoding of RDF IRIs, literals, and blank nodes.
    """

    protobuf_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_UNSPECIFIED
    protobuf_terms: ClassVar[Sequence[str]]

    options: lookups.Options

    def __init__(self, options: lookups.Options | None = None) -> None:
        if options is None:
            options = lookups.Options.big()
        self.options = options
        self._names = lookups.NameEncoder(lookup_size=options.name_lookup_size)
        self._prefixes = lookups.PrefixEncoder(lookup_size=options.prefix_lookup_size)
        self._datatypes = lookups.DatatypeEncoder(
            lookup_size=options.datatype_lookup_size
        )
        self._repeated_terms: dict[str, object] = dict.fromkeys(self.protobuf_terms)
        self.statement = Statement(self.protobuf_terms)

    def is_repeated(self, term: object) -> bool:
        """
        Check if the RDF term was already used in the same position.

        Parameters
        ----------
        term
            RDFLib term object.

        Returns
        -------
        bool
            True if term is repeated in current slot, False otherwise.
        """
        repeated_term = self._repeated_terms[self.statement.current_term]
        if repeated_term == term:
            return True
        self._repeated_terms[self.statement.current_term] = term
        return False

    def encode_options(self, *, frame_logic: streams.Stream) -> None:
        """
        Encode stream options row and add it to the current statement.

        Parameters
        ----------
        frame_logic
            Active frame logic used to construct output frames.
        """
        options = pb.RdfStreamOptions(
            stream_name=self.options.name,
            physical_type=self.protobuf_type,
            generalized_statements=False,
            rdf_star=False,
            max_name_table_size=self.options.name_lookup_size,
            max_prefix_table_size=self.options.prefix_lookup_size,
            max_datatype_table_size=self.options.datatype_lookup_size,
            logical_type=frame_logic.protobuf_type,
            version=1,
        )
        self.statement.add_rows(pb.RdfStreamRow(options=options))

    def split_iri(self, value: str) -> tuple[str, str]:
        """
        Split full IRI string into prefix and local name.

        Parameters
        ----------
        value
            Full IRI string.

        Returns
        -------
        prefix, name
        """
        name = value
        prefix = ""
        for sep in "#", "/":
            prefix, char, name = value.rpartition(sep)
            if char:
                return prefix + char, name
        return prefix, name

    def encode_iri(self, value: str) -> None:
        """
        Encode an IRI RDF term and update statement.

        Emits prefix/name entries if required by lookup.

        Parameters
        ----------
        value
            IRI string.
        """
        prefix, name = self.split_iri(value)
        prefix_id = self._prefixes.index_for_entry(prefix)
        name_id = self._names.index_for_entry(name)
        term_rows = []

        if prefix_id is not None:
            prefix_entry = pb.RdfPrefixEntry(id=prefix_id, value=prefix)
            term_rows.append(pb.RdfStreamRow(prefix=prefix_entry))

        if name_id is not None:
            name_entry = pb.RdfNameEntry(id=name_id, value=name)
            term_rows.append(pb.RdfStreamRow(name=name_entry))

        prefix_id = self._prefixes.index_for_term(prefix)
        name_id = self._names.index_for_term(name)
        iri = pb.RdfIri(prefix_id=prefix_id, name_id=name_id)
        self.statement.set_iri(iri, rows=term_rows)

    def encode_bnode(self, bnode: str) -> None:
        """
        Encode a blank node RDF term.

        Parameters
        ----------
        bnode
            Blank node identifier string.
        """
        self.statement.set_bnode(bnode)

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> None:
        """
        Encode an RDF literal term.

        Parameters
        ----------
        lex
            Lexical form.
        language
            Optional language tag.
        datatype
            Optional datatype IRI.
        """
        datatype_id = None
        term_rows = []

        if datatype:
            datatype_entry_id = self._datatypes.index_for_entry(datatype)

            if datatype_entry_id is not None:
                entry = pb.RdfDatatypeEntry(id=datatype_entry_id, value=datatype)
                term_rows = [pb.RdfStreamRow(datatype=entry)]

            datatype_id = self._datatypes.index_for_term(datatype)

        literal = pb.RdfLiteral(lex=lex, langtag=language, datatype=datatype_id)
        self.statement.set_literal(literal, rows=term_rows)

    def cycle(self, frame_logic: streams.Stream) -> pb.RdfStreamFrame | None:
        """
        Finalize the current term and try to emit a new frame.

        Parameters
        ----------
        frame_logic
            Frame packing strategy.

        Returns
        -------
        RdfStreamFrame or None
            Returns frame if ready, otherwise None.
        """
        frame = None

        if self.statement.current_term == self.protobuf_terms[-1]:
            preceding_rows, terms = self.statement._collect()
            frame = frame_logic.add_row(*preceding_rows, self.row_factory(**terms))

        self.statement._cycle()
        return frame

    @staticmethod
    @abstractmethod
    def row_factory(**terms: Any) -> pb.RdfStreamRow:
        raise NotImplementedError


@final
class TripleEncoder(StatementEncoder):
    """
    Jelly encoder for RDF triples.

    Produces `RdfStreamRow(triple=...)` frames from RDF terms.
    """

    protobuf_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_TRIPLES
    protobuf_terms = ("s", "p", "o")

    @staticmethod
    def row_factory(**terms: Any) -> pb.RdfStreamRow:
        """
        Construct a protobuf stream row from RDF term data.

        Parameters
        ----------
        **terms
            Serialized RDF terms.

        Returns
        -------
        RdfStreamRow
            Protobuf row representation.
        """
        return pb.RdfStreamRow(triple=pb.RdfTriple(**terms))
