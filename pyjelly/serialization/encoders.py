from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from itertools import cycle
from typing import Any, ClassVar

from pyjelly.pb2 import rdf_pb2 as pb
from pyjelly.serialization import lookups, queues


class Statement:
    def __init__(self, encoder: StatementEncoder) -> None:
        self.encoder = encoder
        self.terms: dict[str, Any] = {}
        self._field_cycle = cycle(self.encoder._pb_fields)
        self.field = next(self._field_cycle)

    def _key(self, term: str) -> str:
        return f"{self.field}_{term}"

    def cycle(self, queue: queues.Queue):
        if self.field == self.encoder._pb_fields[-1]:
            self.encoder._encode_row(queue)
            self.terms.clear()
        self.field = next(self._field_cycle)

    def set_term(self, term: str, value: Any) -> None:
        self.terms[self._key(term)] = value


class StatementEncoder(metaclass=ABCMeta):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_UNSPECIFIED
    _pb_fields: ClassVar[Sequence[str]]

    options: lookups.Options
    repeated_terms: dict[str, Any]

    def __init__(self, options: lookups.Options) -> None:
        self.options = options
        self.name_lookup = lookups.NameLookup(size=options.name_lookup_size)
        self.prefix_lookup = lookups.PrefixLookup(size=options.prefix_lookup_size)
        self.datatype_lookup = lookups.DatatypeLookup(size=options.datatype_lookup_size)
        self.repeated_fields: dict[str, object] = {}
        self._statement = Statement(self)

    def should_skip(self, term: object) -> bool:
        repeated_term = self.repeated_fields.get(self._statement.field)
        if repeated_term == term:
            return True

        self.repeated_fields[self._statement.field] = term
        return False

    def encode_options(self, queue: queues.Queue) -> None:
        options = pb.RdfStreamOptions(
            stream_name=self.options.name,
            physical_type=self._pb_type,
            generalized_statements=False,
            rdf_star=False,
            max_name_table_size=self.options.name_lookup_size,
            max_prefix_table_size=self.options.prefix_lookup_size,
            max_datatype_table_size=self.options.datatype_lookup_size,
            logical_type=queue._pb_type,
            version=1,
        )
        queue.add(pb.RdfStreamRow(options=options))

    def split_iri(self, value: str) -> tuple[str, str]:
        name = value
        prefix = ""
        for sep in "#", "/":
            prefix, char, name = value.rpartition(sep)
            if prefix and char:
                return prefix + char, name
        return prefix, name

    def encode_iri(self, value: str, queue: queues.Queue) -> None:
        prefix, name = self.split_iri(value)
        prefix_id = self.prefix_lookup.for_entry(prefix)
        name_id = self.name_lookup.for_entry(name)

        if prefix_id is not None:
            entry = pb.RdfPrefixEntry(id=prefix_id, value=prefix)
            queue.add(pb.RdfStreamRow(prefix=entry))

        if name_id is not None:
            entry = pb.RdfNameEntry(id=name_id, value=name)
            queue.add(pb.RdfStreamRow(name=entry))

        prefix_id = self.prefix_lookup.for_term(prefix)
        name_id = self.name_lookup.for_term(name)

        self._statement.set_term("iri", pb.RdfIri(prefix_id=prefix_id, name_id=name_id))

    def encode_bnode(self, value: str) -> None:
        self._statement.set_term("bnode", value)

    def encode_datatype(self, datatype: str, queue: queues.Queue) -> None:
        datatype_id = self.datatype_lookup.for_entry(datatype)

        if datatype_id is not None:
            entry = pb.RdfDatatypeEntry(id=datatype_id, value=datatype)
            queue.add(pb.RdfStreamRow(datatype=entry))

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
        queue: queues.Queue,
    ) -> None:
        datatype_id = None

        if datatype:
            self.encode_datatype(datatype=datatype, queue=queue)
            datatype_id = self.datatype_lookup.for_term(datatype)

        literal = pb.RdfLiteral(lex=lex, langtag=language, datatype=datatype_id)
        self._statement.set_term("literal", value=literal)

    def cycle(self, queue: queues.Queue) -> None:
        self._statement.cycle(queue)

    def _encode_row(self, queue: queues.Queue) -> None:
        row = self._make_row()
        queue.add(row)

    @abstractmethod
    def _make_row(self) -> pb.RdfStreamRow:
        raise NotImplementedError


class TripleEncoder(StatementEncoder):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_TRIPLES
    _pb_fields = "spo"

    def _make_row(self) -> pb.RdfStreamRow:
        return pb.RdfStreamRow(triple=pb.RdfTriple(**self._statement.terms))


class QuadEncoder(StatementEncoder):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_QUADS
    _pb_fields = "spog"

    @property
    def current_graph(self) -> Any | None:
        return self.repeated_terms.get("g")

    def _make_row(self) -> pb.RdfStreamRow:
        return pb.RdfStreamRow(quad=pb.RdfQuad(**self._statement.terms))


class GraphEncoder(StatementEncoder):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_GRAPHS
    _pb_fields = "spo"

    current_graph: Any | None

    def __init__(self, options: lookups.Options) -> None:
        super().__init__(options)
        self.current_graph = None

    def _make_row(self) -> pb.RdfStreamRow:
        return pb.RdfStreamRow(triple=pb.RdfTriple(**self._statement.terms))
