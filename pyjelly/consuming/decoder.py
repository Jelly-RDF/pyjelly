from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

from pyjelly import jelly
from pyjelly.consuming.lookups import LookupDecoder
from pyjelly.options import ConsumerStreamOptions


class Adapter:
    def __init__(self, options: ConsumerStreamOptions) -> None:
        self.options = options

    def triple(self, terms: Iterable[Any]) -> Any:
        raise NotImplementedError

    def quad(self, terms: Iterable[Any]) -> Any:
        raise NotImplementedError

    def iri(self, namespace: str, iri: str) -> Any:
        raise NotImplementedError

    def default_graph(self) -> Any:
        raise NotImplementedError

    def bnode(self, bnode: str) -> Any:
        raise NotImplementedError

    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> Any:
        raise NotImplementedError

    def graph_start(self, graph_id: Any) -> Any:
        raise NotImplementedError

    def graph_end(self) -> Any:
        raise NotImplementedError

    def namespace_declaration(self, name: str, iri: str) -> Any:
        raise NotImplementedError

    def frame(self) -> Any:
        return None


class Decoder:
    def __init__(self, adapter: Adapter) -> None:
        self.adapter = adapter
        self.names = LookupDecoder(lookup_size=self.options.name_lookup_size)
        self.prefixes = LookupDecoder(lookup_size=self.options.prefix_lookup_size)
        self.datatypes = LookupDecoder(lookup_size=self.options.datatype_lookup_size)
        self.repeated_terms: dict[str, jelly.RdfIri | str | jelly.RdfLiteral] = {}

    @property
    def options(self) -> ConsumerStreamOptions:
        return self.adapter.options

    def decode_frame(self, frame: jelly.RdfStreamFrame) -> Any:
        for row_owner in frame.rows:
            row = getattr(row_owner, row_owner.WhichOneof("row"))
            self.decode_row(row)
        return self.adapter.frame()

    def decode_row(self, row: Any) -> Any | None:
        return self.row_handlers[type(row)](self, row)

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

    def decode_term(self, term: Any) -> Any:
        return self.term_decoders[type(term)](self, term)

    def decode_iri(self, iri: jelly.RdfIri) -> Any:
        name = self.names.decode_name_term_index(iri.name_id)
        prefix = self.prefixes.decode_prefix_term_index(iri.prefix_id)
        return self.adapter.iri(iri=prefix + name)

    def decode_default_graph(self, _: jelly.RdfDefaultGraph) -> Any:
        return self.adapter.default_graph()

    def decode_bnode(self, bnode: str) -> Any:
        return self.adapter.bnode(bnode)

    def decode_literal(self, literal: jelly.RdfLiteral) -> Any:
        language = datatype = None
        if literal.langtag:
            language = literal.langtag
        elif literal.datatype:
            datatype = self.datatypes.decode_datatype_term_index(literal.datatype)
        return self.adapter.literal(
            lex=literal.lex,
            language=language,
            datatype=datatype,
        )

    def decode_namespace_declaration(
        self, declaration: jelly.RdfNamespaceDeclaration
    ) -> Any:
        iri = self.decode_iri(declaration.value)
        return self.adapter.namespace_declaration(declaration.name, iri)

    def decode_graph_start(self, graph_start: jelly.RdfGraphStart) -> Any:
        term = getattr(graph_start, graph_start.WhichOneof("graph"))
        return self.adapter.graph_start(self.decode_term(term))

    def decode_graph_end(self, _: jelly.RdfGraphEnd) -> Any:
        return self.adapter.graph_end()

    def decode_statement(
        self,
        statement: jelly.RdfTriple | jelly.RdfQuad,
        oneofs: Sequence[str],
    ) -> Any:
        terms = []
        for oneof in oneofs:
            field = statement.WhichOneof(oneof)
            if field:
                jelly_term = getattr(statement, field)
                decoded_term = self.decode_term(jelly_term)
                self.repeated_terms[oneof] = decoded_term
            else:
                decoded_term = self.repeated_terms[oneof]
            terms.append(decoded_term)
        return terms

    def decode_triple(self, triple: jelly.RdfTriple) -> Any:
        terms = self.decode_statement(triple, ("subject", "predicate", "object"))
        return self.adapter.triple(terms)

    def decode_quad(self, quad: jelly.RdfQuad) -> Any:
        terms = self.decode_statement(quad, ("subject", "predicate", "object", "graph"))
        return self.adapter.quad(terms)

    # dispatch by invariant type (no C3 resolution)
    row_handlers: ClassVar = {
        jelly.RdfStreamOptions: validate_stream_options,
        jelly.RdfPrefixEntry: ingest_prefix_entry,
        jelly.RdfNameEntry: ingest_name_entry,
        jelly.RdfDatatypeEntry: ingest_datatype_entry,
        jelly.RdfTriple: decode_triple,
        jelly.RdfQuad: decode_quad,
        jelly.RdfGraphStart: decode_graph_start,
        jelly.RdfGraphEnd: decode_graph_end,
        jelly.RdfNamespaceDeclaration: decode_namespace_declaration,
    }

    term_decoders: ClassVar = {
        jelly.RdfIri: decode_iri,
        str: decode_bnode,
        jelly.RdfLiteral: decode_literal,
        jelly.RdfDefaultGraph: decode_default_graph,
    }
