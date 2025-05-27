from __future__ import annotations

from collections.abc import Generator, Iterable
from dataclasses import dataclass, field
from typing import Any, ClassVar

from pyjelly import jelly
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.serialize.encode import (
    Slot,
    TermEncoder,
    encode_namespace_declaration,
    encode_options,
    encode_quad,
    encode_triple,
)
from pyjelly.serialize.flows import FrameFlow


@dataclass
class SerializerOptions:
    flow: FrameFlow
    params: StreamParameters = field(default_factory=StreamParameters)
    lookup_preset: LookupPreset = field(default_factory=LookupPreset)


class Stream:
    physical_type: ClassVar[jelly.PhysicalStreamType]

    def __init__(self, *, encoder: TermEncoder, options: SerializerOptions) -> None:
        self.encoder = encoder
        self.options = options
        self.flow = options.flow
        self.repeated_terms = dict.fromkeys(Slot)
        self.enrolled = False
        self.stream_types = StreamTypes(
            physical_type=self.physical_type,
            logical_type=self.flow.logical_type,
        )

    def enroll(self) -> None:
        if not self.enrolled:
            self.stream_options()
            self.enrolled = True

    def stream_options(self) -> None:
        self.flow.append(
            encode_options(
                stream_types=self.stream_types,
                params=self.options.params,
                lookup_preset=self.options.lookup_preset,
            )
        )

    def namespace_declaration(self, name: str, iri: str) -> None:
        rows = encode_namespace_declaration(
            name=name,
            value=iri,
            term_encoder=self.encoder,
        )
        self.flow.extend(rows)

    @classmethod
    def for_rdflib(cls, options: SerializerOptions) -> Stream:
        if cls is Stream:
            msg = "Stream is an abstract base class, use a subclass instead"
            raise TypeError(msg)
        from pyjelly.integrations.rdflib.serialize import RDFLibTermEncoder

        return cls(
            encoder=RDFLibTermEncoder(lookup_preset=options.lookup_preset),
            options=options,
        )


def stream_for_type(physical_type: jelly.PhysicalStreamType) -> type[Stream]:
    try:
        stream_cls = STREAM_DISPATCH[physical_type]
    except KeyError:
        msg = (
            "no stream class for physical type "
            f"{jelly.PhysicalStreamType.Name(physical_type)}"
        )
        raise NotImplementedError(msg) from None
    return stream_cls


class TripleStream(Stream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES

    def triple(self, terms: Iterable[object]) -> jelly.RdfStreamFrame | None:
        new_rows = encode_triple(
            terms,
            term_encoder=self.encoder,
            repeated_terms=self.repeated_terms,
        )
        self.flow.extend(new_rows)
        if frame := self.flow.frame_from_bounds():
            return frame
        return None


class QuadStream(Stream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_QUADS

    def quad(self, terms: Iterable[object]) -> jelly.RdfStreamFrame | None:
        new_rows = encode_quad(
            terms,
            term_encoder=self.encoder,
            repeated_terms=self.repeated_terms,
        )
        self.flow.extend(new_rows)
        if frame := self.flow.frame_from_bounds():
            return frame
        return None


class GraphStream(TripleStream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_GRAPHS

    def graph(
        self,
        graph_id: object,
        graph: Iterable[Iterable[object]],
    ) -> Generator[jelly.RdfStreamFrame]:
        [*graph_rows], graph_node = self.encoder.encode_any(graph_id, Slot.graph)
        kw_name = f"{Slot.graph}_{self.encoder.TERM_ONEOF_NAMES[type(graph_node)]}"
        kws: dict[Any, Any] = {kw_name: graph_node}
        start_row = jelly.RdfStreamRow(graph_start=jelly.RdfGraphStart(**kws))
        graph_rows.append(start_row)
        self.flow.extend(graph_rows)
        for triple in graph:
            if frame := self.triple(triple):
                yield frame
        end_row = jelly.RdfStreamRow(graph_end=jelly.RdfGraphEnd())
        self.flow.append(end_row)
        if self.flow.frame_from_bounds():
            yield self.flow.to_stream_frame()  # type: ignore[misc]


STREAM_DISPATCH: dict[jelly.PhysicalStreamType, type[Stream]] = {
    jelly.PHYSICAL_STREAM_TYPE_TRIPLES: TripleStream,
    jelly.PHYSICAL_STREAM_TYPE_QUADS: QuadStream,
    jelly.PHYSICAL_STREAM_TYPE_GRAPHS: GraphStream,
}
