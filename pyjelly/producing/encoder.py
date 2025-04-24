from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from itertools import chain
from typing import Any, Literal
from typing_extensions import TypeAlias

from pyjelly import jelly
from pyjelly.options import STRING_DATATYPE_IRI, StreamOptions
from pyjelly.producing.lookups import LookupEncoder

TermName: TypeAlias = Literal["s", "p", "o", "g"]

TERM_ONEOF_NAMES = {
    jelly.RdfIri: "iri",
    jelly.RdfLiteral: "literal",
    str: "bnode",
    jelly.RdfDefaultGraph: "default_graph",
}

STATEMENT_ONEOF_NAMES = {
    jelly.RdfTriple: "triple",
    jelly.RdfQuad: "quad",
}


class Statement:
    """Helper to manage RDF term encoding state during serialization."""

    def __init__(self, *, quads: bool) -> None:
        self.jelly_statement: Any = jelly.RdfQuad if quads else jelly.RdfTriple
        self.row_oneof: Any = STATEMENT_ONEOF_NAMES[self.jelly_statement]
        self.extra_stream_rows: dict[TermName, Iterable[jelly.RdfStreamRow]] = {}
        self.term_values: dict[TermName, jelly.RdfIri | jelly.RdfLiteral | str] = {}
        self.term_types: dict[TermName, str] = {}

    def add_term(
        self,
        name: TermName,
        value: jelly.RdfIri | str | jelly.RdfLiteral,
        rows: Iterable[jelly.RdfStreamRow] = (),
    ) -> None:
        self.extra_stream_rows[name] = rows
        self.term_values[name] = value
        self.term_types[name] = TERM_ONEOF_NAMES[type(value)]

    def to_stream_rows(self) -> tuple[jelly.RdfStreamRow, ...]:
        extra_rows = chain(*self.extra_stream_rows.values())
        fields = {
            f"{term_name}_{term_type}": self.term_values[term_name]
            for term_name, term_type in self.term_types.items()
        }
        self.term_values.clear()
        self.term_types.clear()
        self.extra_stream_rows.clear()
        statement = self.jelly_statement(**fields)
        stream_row = jelly.RdfStreamRow(**{self.row_oneof: statement})
        return (*extra_rows, stream_row)


class Encoder(metaclass=ABCMeta):
    """Base class for Jelly statement encoders."""

    repeated_terms: dict[str, object]

    def __init__(
        self,
        *,
        physical_type: jelly.PhysicalStreamType,
        options: StreamOptions | None = None,
    ) -> None:
        assert physical_type is not jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED
        if options is None:
            options = StreamOptions.big()
        self.physical_type = physical_type
        self.options = options
        self.names = LookupEncoder(lookup_size=options.name_lookup_size)
        self.prefixes = LookupEncoder(lookup_size=options.prefix_lookup_size)
        self.datatypes = LookupEncoder(lookup_size=options.datatype_lookup_size)
        self.repeated_terms = dict.fromkeys("spog")
        self.statement = Statement(
            quads=physical_type is jelly.PHYSICAL_STREAM_TYPE_QUADS
        )

    def is_repeated(self, term: object, name: TermName) -> bool:
        """
        Check if the RDF term was already used in the same position.

        Parameters
        ----------
        term
            RDFLib term object.
        name
            Name of the repeated term.

        Returns
        -------
        bool
            True if term is repeated in current slot, False otherwise.

        """
        repeated_term = self.repeated_terms[name]
        if repeated_term == term:
            return True
        self.repeated_terms[name] = term
        return False

    def options_to_stream_row(
        self,
        *,
        logical_type: jelly.LogicalStreamType,
    ) -> jelly.RdfStreamRow:
        options = jelly.RdfStreamOptions(
            stream_name=self.options.stream_name,
            physical_type=self.physical_type,
            generalized_statements=True,
            rdf_star=False,
            max_name_table_size=self.options.name_lookup_size,
            max_prefix_table_size=self.options.prefix_lookup_size,
            max_datatype_table_size=self.options.datatype_lookup_size,
            logical_type=logical_type,
            version=1,
        )
        return jelly.RdfStreamRow(options=options)

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

    def encode_iri(self, iri: str, *, term_name: TermName) -> None:
        prefix, name = self.split_iri(iri)
        prefix_id = self.prefixes.encode_entry_index(prefix)
        name_id = self.names.encode_entry_index(name)
        term_rows = []

        if prefix_id is not None:
            prefix_entry = jelly.RdfPrefixEntry(id=prefix_id, value=prefix)
            term_rows.append(jelly.RdfStreamRow(prefix=prefix_entry))

        if name_id is not None:
            name_entry = jelly.RdfNameEntry(id=name_id, value=name)
            term_rows.append(jelly.RdfStreamRow(name=name_entry))

        prefix_id = self.prefixes.encode_prefix_term_index(prefix)
        name_id = self.names.encode_name_term_index(name)
        jelly_iri = jelly.RdfIri(prefix_id=prefix_id, name_id=name_id)
        self.statement.add_term(term_name, jelly_iri, rows=term_rows)

    def encode_default_graph(self, *, term_name: TermName) -> None:
        self.statement.add_term(term_name, jelly.RdfDefaultGraph())

    def encode_bnode(self, bnode: str, *, term_name: TermName) -> None:
        self.statement.add_term(term_name, str(bnode))  # invariant str needed

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
        term_name: TermName,
    ) -> None:
        datatype_id = None
        term_rows: tuple[jelly.RdfStreamRow, ...] = ()

        if datatype and datatype != STRING_DATATYPE_IRI:
            datatype_entry_id = self.datatypes.encode_entry_index(datatype)

            if datatype_entry_id is not None:
                entry = jelly.RdfDatatypeEntry(id=datatype_entry_id, value=datatype)
                term_rows = (jelly.RdfStreamRow(datatype=entry),)

            datatype_id = self.datatypes.encode_datatype_term_index(datatype)

        literal = jelly.RdfLiteral(lex=lex, langtag=language, datatype=datatype_id)
        self.statement.add_term(term_name, literal, rows=term_rows)

    def to_stream_rows(self) -> tuple[jelly.RdfStreamRow, ...]:
        return self.statement.to_stream_rows()

    @abstractmethod
    def encode_term(self, term: Any, name: TermName) -> None:
        raise NotImplementedError

    def encode_statement(self, terms: Iterable[object]) -> None:
        name: TermName
        for name, term in zip(("spog"), terms):  # type: ignore[assignment]
            if not self.is_repeated(term, name=name):
                self.encode_term(term, name=name)
