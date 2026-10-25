from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from pyjelly import jelly
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.serialize.encode import (
    Slot,
    Statement,
    TermEncoder,
    encode_namespace_declaration,
    encode_options,
    encode_quad,
    encode_triple,
    write_graph,
    write_term,
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

if TYPE_CHECKING:
    from jelly import LogicalStreamType  # type: ignore[import-not-found]


@dataclass
class SerializerOptions:
    flow: FrameFlow | None = None
    frame_size: int = DEFAULT_FRAME_SIZE
    logical_type: LogicalStreamType = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    params: StreamParameters = field(default_factory=StreamParameters)
    lookup_preset: LookupPreset = field(default_factory=LookupPreset)


@mypyc_attr(allow_interpreted_subclasses=True)
class Stream:
    physical_type: ClassVar[jelly.PhysicalStreamType]
    default_delimited_flow_class: ClassVar[type[BoundedFrameFlow]]
    repeated_terms: list[object | None]

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
        # The encoder adds rows for new lookup entries to the same flow
        encoder.flow = flow
        self.repeated_terms = [None] * len(Slot)
        # See set_subject()
        self.subject_changed = False
        self.predicate_changed = False
        self.object_changed = False
        self.graph_changed = False
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

            if issubclass(flow_class, BoundedFrameFlow) and (
                self.options.logical_type
                in (
                    jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
                    jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
                )
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
        self.flow.new_row().options.CopyFrom(
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
        encode_namespace_declaration(name=name, value=iri, term_encoder=self.encoder)

    # Cursor API: instead of passing whole statements, the caller sets only the
    # terms that changed since the previous statement, and then emits it.
    # This is used when writing from an indexed Graph or Dataset.

    def set_subject(self, s: object) -> None:
        """Set the subject of the next statement."""
        self.repeated_terms[0] = s
        self.subject_changed = True

    def set_predicate(self, p: object) -> None:
        """Set the predicate of the next statement."""
        self.repeated_terms[1] = p
        self.predicate_changed = True

    def set_object(self, o: object) -> None:
        """Set the object of the next statement."""
        self.repeated_terms[2] = o
        self.object_changed = True

    def emit(self) -> jelly.RdfStreamFrame | None:
        """
        Add a statement made of the terms set with the cursor API.

        Returns:
            jelly.RdfStreamFrame | None: stream frame if
                flow supports frames slicing and current flow is full

        """
        raise NotImplementedError

    def encode_changed_spo(self) -> int:
        """
        Encode the subject, predicate and object that were set since the last emit.

        They are encoded in this order no matter in which order they were set:
        IRIs are encoded relative to the IRI before them, in the order the parser
        reads them. Rows for new lookup entries are added to the flow.

        Returns:
            int: which of them were encoded, as bits 0 (subject) to 2 (object).

        """
        terms = self.repeated_terms
        encoded = self.encoder.encoded
        changed = 0
        if self.subject_changed:
            self.encoder.encode_spo(terms[0], encoded[0])
            self.subject_changed = False
            changed = 1
        if self.predicate_changed:
            self.encoder.encode_spo(terms[1], encoded[1])
            self.predicate_changed = False
            changed |= 2
        if self.object_changed:
            self.encoder.encode_spo(terms[2], encoded[2])
            self.object_changed = False
            changed |= 4
        return changed

    def write_changed_spo(self, statement: Statement, changed: int) -> None:
        """Write the terms encoded by encode_changed_spo() into the statement."""
        encoded = self.encoder.encoded
        if not changed:
            # Every term is repeated: the row is an empty statement
            statement.SetInParent()
        if changed & 1:
            write_term(encoded[0], statement, 0)
        if changed & 2:
            write_term(encoded[1], statement, 1)
        if changed & 4:
            write_term(encoded[2], statement, 2)

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
        from pyjelly.integrations.rdflib.encode import (  # noqa: PLC0415
            RDFLibTermEncoder,
        )

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
        if physical_type == jelly.PHYSICAL_STREAM_TYPE_GRAPHS:
            msg = (
                "writing streams of the GRAPHS physical type is not supported "
                "(they can still be read). Use QUADS instead."
            )
        raise NotImplementedError(msg) from None
    return stream_cls


class TripleStream(Stream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    default_delimited_flow_class: ClassVar[type[BoundedFrameFlow]] = (
        FlatTriplesFrameFlow
    )

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
        encode_triple(terms, self.encoder, self.repeated_terms)
        return self.flow.frame_from_bounds()

    def emit(self) -> jelly.RdfStreamFrame | None:
        """
        Add a triple made of the terms set with the cursor API.

        Returns:
            jelly.RdfStreamFrame | None: stream frame if
                flow supports frames slicing and current flow is full

        """
        self.encoder.new_epoch()
        # The terms first, as they add rows for new lookup entries
        changed = self.encode_changed_spo()
        self.write_changed_spo(self.flow.new_row().triple, changed)
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
        encode_quad(terms, self.encoder, self.repeated_terms)
        return self.flow.frame_from_bounds()

    def set_graph(self, g: object) -> None:
        """Set the graph name of the next quad (see Stream.set_subject)."""
        self.repeated_terms[3] = g
        self.graph_changed = True

    def emit(self) -> jelly.RdfStreamFrame | None:
        """
        Add a quad made of the terms set with the cursor API.

        Returns:
            jelly.RdfStreamFrame | None: stream frame if
                flow supports frames slicing and current flow is full

        """
        self.encoder.new_epoch()
        # The terms first, as they add rows for new lookup entries. The graph
        # comes last, as in the parser.
        changed = self.encode_changed_spo()
        graph_changed = self.graph_changed
        if graph_changed:
            self.encoder.encode_graph(self.repeated_terms[3], self.encoder.encoded[3])
            self.graph_changed = False
            changed |= 8
        quad = self.flow.new_row().quad
        self.write_changed_spo(quad, changed)
        if graph_changed:
            write_graph(self.encoder.encoded[3], quad)
        return self.flow.frame_from_bounds()


STREAM_DISPATCH: dict[jelly.PhysicalStreamType, type[Stream]] = {
    jelly.PHYSICAL_STREAM_TYPE_TRIPLES: TripleStream,
    jelly.PHYSICAL_STREAM_TYPE_QUADS: QuadStream,
}
