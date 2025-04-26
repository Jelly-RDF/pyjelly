from __future__ import annotations

from collections.abc import Generator, Iterable, Sequence
from typing import Any, ClassVar

from pyjelly import jelly
from pyjelly.consuming.lookups import LookupDecoder
from pyjelly.options import StreamOptions


class Decoder:
    def __init__(self, options: StreamOptions) -> None:
        self.options = options
        self.names = LookupDecoder(lookup_size=options.name_lookup_size)
        self.prefixes = LookupDecoder(lookup_size=options.prefix_lookup_size)
        self.datatypes = LookupDecoder(lookup_size=options.datatype_lookup_size)
        self.repeated_terms: dict[str, jelly.RdfIri | str | jelly.RdfLiteral] = {}

    def decode_stream_frame(self, frame: jelly.RdfStreamFrame) -> Generator[Any]:
        for row_owner in frame.rows:
            row = getattr(row_owner, row_owner.WhichOneof("row"))
            returned = self.row_handlers[type(row)](self, row)
            if returned is not None:
                yield returned

    def validate_stream_options(self, options: jelly.RdfStreamOptions) -> None:
        assert self.options.stream_name == options.stream_name
        assert self.options.version >= options.version
        assert self.options.prefix_lookup_size == options.max_prefix_table_size
        assert self.options.datatype_lookup_size == options.max_datatype_table_size
        assert self.options.name_lookup_size == options.max_name_table_size

    def ingest_prefix_entry(self, entry: jelly.RdfPrefixEntry) -> None:
        self.prefixes.assign_entry(index=entry.id, value=entry.value)

    def ingest_name_entry(self, entry: jelly.RdfNameEntry) -> None:
        self.names.assign_entry(index=entry.id, value=entry.value)

    def ingest_datatype_entry(self, entry: jelly.RdfDatatypeEntry) -> None:
        self.datatypes.assign_entry(index=entry.id, value=entry.value)

    def decode_statement(
        self, statement: jelly.RdfTriple | jelly.RdfQuad, fields: Sequence[str]
    ) -> Any:
        terms = []
        for term_name in fields:
            field = statement.WhichOneof(term_name)
            if field:
                jelly_term = getattr(statement, field)
                decoded_term = self.term_decoders[type(jelly_term)](self, jelly_term)
                self.repeated_terms[term_name] = decoded_term
            else:
                decoded_term = self.repeated_terms[term_name]
            terms.append(decoded_term)
        return self.transform_statement(terms)

    def decode_triple(self, triple: jelly.RdfTriple) -> Any:
        return self.decode_statement(triple, ("subject", "predicate", "object"))

    def decode_quad(self, quad: jelly.RdfQuad) -> Any:
        return self.decode_statement(quad, ("subject", "predicate", "object", "graph"))

    def decode_iri(self, iri: jelly.RdfIri) -> Any:
        name = self.names.decode_name_term_index(iri.name_id)
        prefix = self.prefixes.decode_prefix_term_index(iri.prefix_id)
        return self.transform_iri(iri=prefix + name)

    def decode_bnode(self, bnode: str) -> Any:
        return self.transform_bnode(bnode)

    def decode_literal(self, literal: jelly.RdfLiteral) -> Any:
        language = datatype = None
        if literal.langtag:
            language = literal.langtag
        elif literal.datatype:
            datatype = self.datatypes.decode_datatype_term_index(literal.datatype)
        return self.transform_literal(
            lex=literal.lex,
            language=language,
            datatype=datatype,
        )

    def transform_statement(self, terms: Iterable[Any]) -> Any:
        return tuple(terms)

    def transform_iri(self, iri: str) -> Any:
        raise NotImplementedError

    def transform_bnode(self, bnode: str) -> Any:
        raise NotImplementedError

    def transform_literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> Any:
        raise NotImplementedError

    row_handlers: ClassVar[dict[Any, Any]] = {
        jelly.RdfStreamOptions: validate_stream_options,
        jelly.RdfPrefixEntry: ingest_prefix_entry,
        jelly.RdfNameEntry: ingest_name_entry,
        jelly.RdfDatatypeEntry: ingest_datatype_entry,
        jelly.RdfTriple: decode_triple,
        jelly.RdfQuad: decode_quad,
    }

    term_decoders: ClassVar = {
        jelly.RdfIri: decode_iri,
        str: decode_bnode,
        jelly.RdfLiteral: decode_literal,
    }
