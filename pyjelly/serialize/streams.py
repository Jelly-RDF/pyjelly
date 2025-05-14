from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import Any, ClassVar

from pyjelly import jelly
from pyjelly.options import StreamOptions
from pyjelly.serialize.encode import (
    Slot,
    TermEncoder,
    encode_namespace_declaration,
    encode_options,
    encode_quad,
    encode_triple,
    new_repeated_terms,
)
from pyjelly.serialize.flows import FrameFlow, ManualFrameFlow


class Stream:
    physical_type: ClassVar[jelly.PhysicalStreamType]
    registry: ClassVar[dict[jelly.PhysicalStreamType, type[Stream]]] = {}
    flow: FrameFlow

    def __init__(
        self,
        *,
        options: StreamOptions,
        encoder_class: type[TermEncoder],
        **flow_args: Any,
    ) -> None:
        assert options.stream_types.physical_type == self.physical_type
        self.options = options
        self.encoder = encoder_class(
            max_prefixes=options.lookup_preset.max_prefixes,
            max_names=options.lookup_preset.max_names,
            max_datatypes=options.lookup_preset.max_datatypes,
        )
        flow_class = FrameFlow.registry[self.options.stream_types.logical_type]
        if not options.delimited:
            flow_class = ManualFrameFlow
        self.flow = flow_class(**flow_args)
        self.repeated_terms = new_repeated_terms()
        self.enrolled = False

    @staticmethod
    def from_options(
        options: StreamOptions,
        encoder_class: type[TermEncoder] | None = None,
        **flow_args: Any,
    ) -> Any:
        if encoder_class is None:
            from pyjelly.integrations.rdflib.serialize import RDFLibTermEncoder

            encoder_class = RDFLibTermEncoder
        stream_class = Stream.registry[options.stream_types.physical_type]
        return stream_class(
            options=options,
            encoder_class=encoder_class,
            **flow_args,
        )

    def enroll(self) -> None:
        if not self.enrolled:
            self.stream_options()
            self.enrolled = True

    def stream_options(self) -> None:
        self.flow.append(encode_options(self.options))

    def namespace_declaration(self, name: str, iri: str) -> None:
        rows = encode_namespace_declaration(
            name=name,
            value=iri,
            term_encoder=self.encoder,
        )
        self.flow.extend(rows)

    def __init_subclass__(cls) -> None:
        cls.registry[cls.physical_type] = cls


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
