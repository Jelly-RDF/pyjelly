from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import cycle
from typing import Any, ClassVar, final
from typing_extensions import Self

from pyjelly._pb2 import rdf_pb2 as pb
from pyjelly._serializing import lookups, streams


@dataclass(frozen=True)
class Options:
    name_lookup_size: int
    prefix_lookup_size: int
    datatype_lookup_size: int
    delimited: bool | None = None
    name: str | None = None

    MIN_NAME_LOOKUP_SIZE: ClassVar[int] = 8

    def __post_init__(self) -> None:
        assert self.name_lookup_size >= self.MIN_NAME_LOOKUP_SIZE, (
            "name lookup size must be at least 8"
        )

    @classmethod
    def small(cls) -> Self:
        return cls(
            name_lookup_size=128,
            prefix_lookup_size=32,
            datatype_lookup_size=32,
        )

    @classmethod
    def big(cls) -> Self:
        return cls(
            name_lookup_size=4000,
            prefix_lookup_size=150,
            datatype_lookup_size=32,
        )


class Statement:
    """
    Helper to manage RDF term encoding state during serialization.

    Collects RDF terms and associated rows before constructing a protobuf
    representation. Supports cycling through terms (s/p/o) and tracking
    repeated terms to optimize serialization.
    """

    _preceding_rows: list[pb.RdfStreamRow]
    _term_rows: Sequence[pb.RdfStreamRow]

    _field_pointer: str

    def __init__(self, terms: Sequence[str], break_at: int = -1) -> None:
        self._preceding_rows = []
        self._term_rows = ()

        self._break_at_field = terms[break_at]
        self._all_fields = cycle(terms)
        self._field_pointer = next(self._all_fields)

        self._values: dict[str, object] = {}

    @property
    def ready(self) -> bool:
        return self._field_pointer == self._break_at_field

    def add_rows(self, *rows: pb.RdfStreamRow) -> None:
        """
        Add supporting rows preceding the current RDF term.

        Parameters
        ----------
        rows
            Stream rows to prepend before current term data.

        """
        self._preceding_rows.extend(rows)

    def set_iri(
        self,
        value: object,
        extra_rows: Sequence[pb.RdfStreamRow] = (),
    ) -> None:
        self._set("iri", value, extra_rows)

    def set_bnode(
        self,
        value: object,
        extra_rows: Sequence[pb.RdfStreamRow] = (),
    ) -> None:
        self._set("bnode", value, extra_rows)

    def set_literal(
        self,
        value: object,
        extra_rows: Sequence[pb.RdfStreamRow] = (),
    ) -> None:
        self._set("literal", value, extra_rows)

    def _set(
        self,
        term_type: str,
        value: object,
        rows: Sequence[pb.RdfStreamRow] = (),
    ) -> None:
        """
        Register a term value and its related rows for the current RDF position.

        Parameters
        ----------
        term_type
            Logical RDF value kind (e.g. 'iri', 'literal').
        value
            Protobuf message or string.
        rows
            Additional stream rows required for encoding.

        """
        self._term_rows = rows
        self._values[f"{self._field_pointer}_{term_type}"] = value

    def _next(self) -> None:
        """
        Advance to the next RDF term in the encoding cycle.

        Moves current term's rows to preceding, clears buffers,
        and updates internal term pointer.
        """
        self._preceding_rows.extend(self._term_rows)
        self._term_rows = ()
        self._field_pointer = next(self._all_fields)

    def _collect(self) -> tuple[tuple[pb.RdfStreamRow, ...], dict[str, Any]]:
        """
        Finalize term collection and return all buffered data.

        Returns
        -------
        tuple of rows and serialized RDF term data.

        """
        term_values = self._values.copy()
        self._values.clear()
        preceding_rows = (*self._preceding_rows, *self._term_rows)
        self._preceding_rows.clear()
        self._term_rows = ()
        return preceding_rows, term_values


class StatementEncoder(metaclass=ABCMeta):
    """
    Base class for Jelly statement encoders.

    Provides lookup logic, repeated term optimization,
    and encoding of RDF IRIs, literals, and blank nodes.
    """

    STRING_DATATYPE_IRI = "http://www.w3.org/2001/XMLSchema#string"

    protobuf_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_UNSPECIFIED
    protobuf_terms: ClassVar[Sequence[str]]

    options: Options

    def __init__(self, options: Options | None = None) -> None:
        if options is None:
            options = Options.big()
        self.options = options
        self._names = lookups.LookupEncoder(lookup_size=options.name_lookup_size)
        self._prefixes = lookups.LookupEncoder(lookup_size=options.prefix_lookup_size)
        self._datatypes = lookups.LookupEncoder(
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
        repeated_term = self._repeated_terms[self.statement._field_pointer]
        if repeated_term == term:
            return True
        self._repeated_terms[self.statement._field_pointer] = term
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
        prefix_id = self._prefixes.encode_entry_index(prefix)
        name_id = self._names.encode_entry_index(name)
        term_rows = []

        if prefix_id is not None:
            prefix_entry = pb.RdfPrefixEntry(id=prefix_id, value=prefix)
            term_rows.append(pb.RdfStreamRow(prefix=prefix_entry))

        if name_id is not None:
            name_entry = pb.RdfNameEntry(id=name_id, value=name)
            term_rows.append(pb.RdfStreamRow(name=name_entry))

        prefix_id = self._prefixes.encode_prefix_term_index(prefix)
        name_id = self._names.encode_name_term_index(name)
        iri = pb.RdfIri(prefix_id=prefix_id, name_id=name_id)
        self.statement.set_iri(iri, extra_rows=term_rows)

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

        if datatype and datatype != self.STRING_DATATYPE_IRI:
            datatype_entry_id = self._datatypes.encode_entry_index(datatype)

            if datatype_entry_id is not None:
                entry = pb.RdfDatatypeEntry(id=datatype_entry_id, value=datatype)
                term_rows = [pb.RdfStreamRow(datatype=entry)]

            datatype_id = self._datatypes.encode_datatype_term_index(datatype)

        literal = pb.RdfLiteral(lex=lex, langtag=language, datatype=datatype_id)
        self.statement.set_literal(literal, extra_rows=term_rows)

    def get_frame_if_ready(
        self,
        frame_logic: streams.Stream,
    ) -> pb.RdfStreamFrame | None:
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

        if self.statement.ready:
            preceding_rows, terms = self.statement._collect()
            frame = frame_logic.add_row(*preceding_rows, self.row_factory(**terms))

        self.statement._next()
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
