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
from pyjelly.serialize.flows import (
    DEFAULT_FRAME_SIZE,
    BoundedFrameFlow,
    FlatQuadsFrameFlow,
    FlatTriplesFrameFlow,
    FrameFlow,
    ManualFrameFlow,
    flow_for_type,
)


@dataclass
class SerializerOptions:
    flow: FrameFlow | None = None
    frame_size: int = DEFAULT_FRAME_SIZE
    logical_type: jelly.LogicalStreamType = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    params: StreamParameters = field(default_factory=StreamParameters)
    lookup_preset: LookupPreset = field(default_factory=LookupPreset)


class Stream:
    physical_type: ClassVar[jelly.PhysicalStreamType]
    default_delimited_flow_class: ClassVar[type[BoundedFrameFlow]]

    def __init__(
        self,
        *,
        encoder: TermEncoder,
        options: SerializerOptions | None = None,
    ) -> None:
        self.encoder = encoder
        if options is None:
            options = SerializerOptions()
        self.options = options
        flow = options.flow
        if flow is None:
            flow = self.infer_flow()
        self.flow = flow
        self.repeated_terms = dict.fromkeys(Slot)
        self.enrolled = False
        self.stream_types = StreamTypes(
            physical_type=self.physical_type,
            logical_type=self.flow.logical_type,
        )

    def infer_flow(self) -> FrameFlow:
        """
        Return flow based on the stream options provided.

        Returns:
            FrameFlow: initialised FrameFlow object.

        """
        flow: FrameFlow
        if self.options.params.delimited:
            if self.options.logical_type != jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED:
                flow_class = flow_for_type(self.options.logical_type)
            else:
                flow_class = self.default_delimited_flow_class

            if self.options.logical_type in (
                jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
                jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            ):
                flow = flow_class(
                    logical_type=self.options.logical_type,
                    frame_size=self.options.frame_size,
                )
            else:
                flow = flow_class(logical_type=self.options.logical_type)
        else:
            flow = ManualFrameFlow(logical_type=self.options.logical_type)
        return flow

    def enroll(self) -> None:
        """Initialize start of the stream."""
        if not self.enrolled:
            self.stream_options()
            self.enrolled = True

    def stream_options(self) -> None:
        """Encode and append stream options row to the current flow."""
        self.flow.append(
            encode_options(
                stream_types=self.stream_types,
                params=self.options.params,
                lookup_preset=self.options.lookup_preset,
            )
        )

    def namespace_declaration(self, name: str, iri: str) -> None:
        """
        Add namespace declaration to jelly stream.

        Args:
            name (str): namespace prefix label
            iri (str): namespace iri

        """
        rows = encode_namespace_declaration(
            name=name,
            value=iri,
            term_encoder=self.encoder,
        )
        self.flow.extend(rows)

    @classmethod
    def for_rdflib(cls, options: SerializerOptions | None = None) -> Stream:
        """
        Initialize stream with RDFLib encoder.

        Args:
            options (SerializerOptions | None, optional): Stream options.
                Defaults to None.

        Raises:
            TypeError: if Stream is passed, and not a Stream for specific physical type.

        Returns:
            Stream: initialized stream with RDFLib encoder.

        """
        if cls is Stream:
            msg = "Stream is an abstract base class, use a subclass instead"
            raise TypeError(msg)
        from pyjelly.integrations.rdflib.serialize import RDFLibTermEncoder

        lookup_preset: LookupPreset | None = None
        if options is not None:
            lookup_preset = options.lookup_preset
        return cls(
            encoder=RDFLibTermEncoder(lookup_preset=lookup_preset),
            options=options,
        )


def stream_for_type(physical_type: jelly.PhysicalStreamType) -> type[Stream]:
    """
    Give a Stream based on physical type specified.

    Args:
        physical_type (jelly.PhysicalStreamType): jelly stream physical type.

    Raises:
        NotImplementedError: if no stream for requested physical type is available.

    Returns:
        type[Stream]: jelly stream

    """
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
    default_delimited_flow_class: ClassVar = FlatTriplesFrameFlow

    def triple(self, terms: Iterable[object]) -> jelly.RdfStreamFrame | None:
        """
        Process one triple to Protobuf messages.

        Note:
            Adds new rows to the current flow and returns StreamFrame if
            frame size conditions are met.

        Args:
            terms (Iterable[object]): RDF terms to encode.

        Returns:
            jelly.RdfStreamFrame | None: stream frame if
                flow supports frames slicing and current flow is full

        """
        new_rows = encode_triple(
            terms,
            term_encoder=self.encoder,
            repeated_terms=self.repeated_terms,
        )
        self.flow.extend(new_rows)
        return self.flow.frame_from_bounds()


class QuadStream(Stream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_QUADS
    default_delimited_flow_class: ClassVar = FlatQuadsFrameFlow

    def quad(self, terms: Iterable[object]) -> jelly.RdfStreamFrame | None:
        """
        Process one quad to Protobuf messages.

        Args:
            terms (Iterable[object]): terms to encode.

        Returns:
            jelly.RdfStreamFrame | None: stream frame if
                flow supports frames slicing and current flow is full

        """
        new_rows = encode_quad(
            terms,
            term_encoder=self.encoder,
            repeated_terms=self.repeated_terms,
        )
        self.flow.extend(new_rows)
        return self.flow.frame_from_bounds()


class GraphStream(TripleStream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_GRAPHS
    default_delimited_flow_class: ClassVar = FlatQuadsFrameFlow

    def graph(
        self,
        graph_id: object,
        graph: Iterable[Iterable[object]],
    ) -> Generator[jelly.RdfStreamFrame]:
        """
        Process one graph into a sequence of jelly frames.

        Args:
            graph_id (object): graph id (BN, Literal, iri, default)
            graph (Iterable[Iterable[object]]): iterable of triples (graph's content)

        Yields:
            Generator[jelly.RdfStreamFrame]: jelly frames.

        """
        [*graph_rows], graph_node = self.encoder.encode_any(graph_id, Slot.graph)
        kw_name = f"{Slot.graph}_{self.encoder.TERM_ONEOF_NAMES[type(graph_node)]}"
        kws: dict[Any, Any] = {kw_name: graph_node}
        start_row = jelly.RdfStreamRow(graph_start=jelly.RdfGraphStart(**kws))
        graph_rows.append(start_row)
        self.flow.extend(graph_rows)
        for triple in graph:
            if frame := self.triple(triple):  # has frame slicing inside
                yield frame
        end_row = jelly.RdfStreamRow(graph_end=jelly.RdfGraphEnd())
        self.flow.append(end_row)
        if frame := self.flow.frame_from_bounds():
            yield frame


STREAM_DISPATCH: dict[jelly.PhysicalStreamType, type[Stream]] = {
    jelly.PHYSICAL_STREAM_TYPE_TRIPLES: TripleStream,
    jelly.PHYSICAL_STREAM_TYPE_QUADS: QuadStream,
    jelly.PHYSICAL_STREAM_TYPE_GRAPHS: GraphStream,
}
