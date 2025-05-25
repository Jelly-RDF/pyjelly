from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar, NamedTuple
from typing_extensions import Never

from pyjelly import jelly
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.parse.lookup import LookupDecoder


class ParserOptions(NamedTuple):
    stream_types: StreamTypes
    lookup_preset: LookupPreset
    params: StreamParameters


def options_from_frame(
    frame: jelly.RdfStreamFrame,
    *,
    delimited: bool,
) -> ParserOptions:
    row = frame.rows[0]
    options = row.options
    return ParserOptions(
        stream_types=StreamTypes(
            physical_type=options.physical_type,
            logical_type=options.logical_type,
        ),
        lookup_preset=LookupPreset(
            max_names=options.max_name_table_size,
            max_prefixes=options.max_prefix_table_size,
            max_datatypes=options.max_datatype_table_size,
        ),
        params=StreamParameters(
            stream_name=options.stream_name,
            version=options.version,
            delimited=delimited,
        ),
    )


def _adapter_missing(feature: str, *, stream_types: StreamTypes) -> Never:
    physical_type_name = jelly.PhysicalStreamType.Name(stream_types.physical_type)
    logical_type_name = jelly.LogicalStreamType.Name(stream_types.logical_type)
    msg = (
        f"adapter with {physical_type_name} and {logical_type_name} "
        f"does not implement {feature}"
    )
    raise NotImplementedError(msg)


class Adapter(metaclass=ABCMeta):
    def __init__(self, options: ParserOptions) -> None:
        self.options = options

    # Obligatory abstract methods--all adapters must implement these
    @abstractmethod
    def iri(self, iri: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def default_graph(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def bnode(self, bnode: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> Any:
        raise NotImplementedError

    # Optional abstract methods--not required to be implemented by all adapters
    def triple(self, terms: Iterable[Any]) -> Any:  # noqa: ARG002
        _adapter_missing("decoding triples", stream_types=self.options.stream_types)

    def quad(self, terms: Iterable[Any]) -> Any:  # noqa: ARG002
        _adapter_missing("decoding quads", stream_types=self.options.stream_types)

    def graph_start(self, graph_id: Any) -> Any:  # noqa: ARG002
        _adapter_missing(
            "decoding graph start markers", stream_types=self.options.stream_types
        )

    def graph_end(self) -> Any:
        _adapter_missing(
            "decoding graph end markers", stream_types=self.options.stream_types
        )

    def namespace_declaration(self, name: str, iri: str) -> Any:  # noqa: ARG002
        _adapter_missing(
            "decoding namespace declarations",
            stream_types=self.options.stream_types,
        )

    def frame(self) -> Any:
        return None


class Decoder:
    def __init__(self, adapter: Adapter) -> None:
        self.adapter = adapter
        self.names = LookupDecoder(lookup_size=self.options.lookup_preset.max_names)
        self.prefixes = LookupDecoder(
            lookup_size=self.options.lookup_preset.max_prefixes
        )
        self.datatypes = LookupDecoder(
            lookup_size=self.options.lookup_preset.max_datatypes
        )
        self.repeated_terms: dict[str, jelly.RdfIri | str | jelly.RdfLiteral] = {}

    @property
    def options(self) -> ParserOptions:
        return self.adapter.options

    def decode_frame(self, frame: jelly.RdfStreamFrame) -> Any:
        for row_owner in frame.rows:
            row = getattr(row_owner, row_owner.WhichOneof("row"))
            self.decode_row(row)
        return self.adapter.frame()

    def decode_row(self, row: Any) -> Any | None:
        try:
            decode_row = self.row_handlers[type(row)]
        except KeyError:
            msg = f"decoder not implemented for {type(row)}"
            raise TypeError(msg) from None
        return decode_row(self, row)

    def validate_stream_options(self, options: jelly.RdfStreamOptions) -> None:
        stream_types, lookup_preset, params = self.options
        assert stream_types.physical_type == options.physical_type
        assert stream_types.logical_type == options.logical_type
        assert params.stream_name == options.stream_name
        assert params.version >= options.version
        assert lookup_preset.max_prefixes == options.max_prefix_table_size
        assert lookup_preset.max_datatypes == options.max_datatype_table_size
        assert lookup_preset.max_names == options.max_name_table_size

    def ingest_prefix_entry(self, entry: jelly.RdfPrefixEntry) -> None:
        self.prefixes.assign_entry(index=entry.id, value=entry.value)

    def ingest_name_entry(self, entry: jelly.RdfNameEntry) -> None:
        self.names.assign_entry(index=entry.id, value=entry.value)

    def ingest_datatype_entry(self, entry: jelly.RdfDatatypeEntry) -> None:
        self.datatypes.assign_entry(index=entry.id, value=entry.value)

    def decode_term(self, term: Any) -> Any:
        try:
            decode_term = self.term_handlers[type(term)]
        except KeyError:
            msg = f"decoder not implemented for {type(term)}"
            raise TypeError(msg) from None
        return decode_term(self, term)

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
        elif self.datatypes.lookup_size and literal.HasField("datatype"):
            datatype = self.datatypes.decode_datatype_term_index(literal.datatype)
        return self.adapter.literal(
            lex=literal.lex,
            language=language,
            datatype=datatype,
        )

    def decode_namespace_declaration(
        self,
        declaration: jelly.RdfNamespaceDeclaration,
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
                if decoded_term is None:
                    msg = f"missing repeated term {oneof}"
                    raise ValueError(msg)
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

    term_handlers: ClassVar = {
        jelly.RdfIri: decode_iri,
        str: decode_bnode,
        jelly.RdfLiteral: decode_literal,
        jelly.RdfDefaultGraph: decode_default_graph,
    }
