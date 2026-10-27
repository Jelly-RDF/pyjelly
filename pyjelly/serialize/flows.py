from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar
from typing_extensions import override

from pyjelly import jelly

if TYPE_CHECKING:
    from pyjelly.serialize.columns import ColumnBatch

DEFAULT_FRAME_SIZE = 1024


class FrameFlow:
    """
    Abstract base class for producing Jelly frames from RDF stream rows.

    Rows are added straight into the frame being built (see `new_row()`), so they
    never have to be copied: protobuf copies a message by serializing it and
    parsing it again.

    In Jelly-RDF 1.2 streams, the frame's statements are in its column batch (see
    `batch`), and the rows only store the stream options. A frame may then also
    end because the lookups are full.

    Allows for passing LogicalStreamType, required for
        logical subtypes and non-delimited streams.
    """

    logical_type: jelly.LogicalStreamType
    #: The logical type of flows of this class, unless given when making one
    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    )
    registry: ClassVar[dict[jelly.LogicalStreamType, type[FrameFlow]]] = {}

    def __init__(
        self,
        *,
        logical_type: jelly.LogicalStreamType | None = None,
    ) -> None:
        self.logical_type = logical_type or self.default_logical_type
        self.frame = jelly.RdfStreamFrame()
        self.rows = self.frame.rows
        #: Set by streams of Jelly-RDF 1.2: the statements of the frame being built
        self.batch: ColumnBatch | None = None

    def new_row(self) -> jelly.RdfStreamRow:
        """Add an empty row at the end of the current frame, and return it."""
        return self.rows.add()

    def __len__(self) -> int:
        """Return how many rows (in Jelly-RDF 1.2, statements) the frame has."""
        batch = self.batch
        if batch is not None:
            return batch.row_count
        return len(self.rows)

    def frame_from_graph(self) -> jelly.RdfStreamFrame | None:
        """
        End a graph: in a stream of RDF Messages, end its message.

        Default implementation returns None: the frame goes on.
        """
        self.end_message()
        return None

    def frame_from_dataset(self) -> jelly.RdfStreamFrame | None:
        """
        End a dataset: in a stream of RDF Messages, end its message.

        Default implementation returns None: the frame goes on.
        """
        self.end_message()
        return None

    def end_message(self) -> None:
        """End the current RDF Message, in a stream of RDF Messages."""
        batch = self.batch
        if batch is not None and batch.messages:
            batch.end_message()

    def frame_from_bounds(self) -> jelly.RdfStreamFrame | None:
        """
        Produce a frame if no more statements fit in it.

        Only in Jelly-RDF 1.2, where the lookups must fit all the frame's entries.
        """
        batch = self.batch
        if batch is not None and not batch.has_room():
            return self.to_stream_frame()
        return None

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        """
        Return the current frame, and start a new one.

        Returns:
            jelly.RdfStreamFrame | None: stream frame, None if it is empty

        """
        batch = self.batch
        if batch is not None and batch.has_content():
            batch.write(self.frame.columns)
        elif not len(self.rows):
            return None
        frame = self.frame
        self.frame = jelly.RdfStreamFrame()
        self.rows = self.frame.rows
        return frame


class ManualFrameFlow(FrameFlow):
    """
    Produces frames only when manually requested (never automatically).

    !!! warning
        All stream rows are kept in memory until `to_stream_frame()` is called.
        This may lead to high memory usage for large streams.

    Used for non-delimited serialization.
    """

    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    )


class BoundedFrameFlow(FrameFlow):
    """
    Produce frames automatically when a fixed number of rows is reached.

    In Jelly-RDF 1.2, it is a number of statements. Used for delimited encoding
    (default mode).
    """

    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    )
    frame_size: int

    def __init__(
        self,
        *,
        logical_type: jelly.LogicalStreamType | None = None,
        frame_size: int | None = None,
    ) -> None:
        super().__init__(logical_type=logical_type)
        self.frame_size = frame_size or DEFAULT_FRAME_SIZE

    @override
    def frame_from_bounds(self) -> jelly.RdfStreamFrame | None:
        """
        Emit frame from flow if full.

        Returns:
            jelly.RdfStreamFrame | None: stream frame

        """
        batch = self.batch
        if batch is None:
            if len(self.rows) >= self.frame_size:
                return self.to_stream_frame()
        elif batch.row_count >= self.frame_size or not batch.has_room():
            return self.to_stream_frame()
        return None


class FlatTriplesFrameFlow(BoundedFrameFlow):
    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
    )


class FlatQuadsFrameFlow(BoundedFrameFlow):
    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
    )


class GraphsFrameFlow(FrameFlow):
    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_GRAPHS
    )

    def frame_from_graph(self) -> jelly.RdfStreamFrame | None:
        """
        Emit current flow content (one graph) as jelly frame.

        Returns:
            jelly.RdfStreamFrame | None: jelly frame or none if
                flow is empty.

        """
        self.end_message()
        return self.to_stream_frame()


class DatasetsFrameFlow(FrameFlow):
    default_logical_type: ClassVar[jelly.LogicalStreamType] = (
        jelly.LOGICAL_STREAM_TYPE_DATASETS
    )

    def frame_from_dataset(self) -> jelly.RdfStreamFrame | None:
        """
        Emit current flow content (dataset) as jelly frame.

        Returns:
            jelly.RdfStreamFrame | None: jelly frame or none if
                flow is empty.

        """
        self.end_message()
        return self.to_stream_frame()


FLOW_DISPATCH: dict[jelly.LogicalStreamType, type[FrameFlow]] = {
    jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES: FlatTriplesFrameFlow,
    jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS: FlatQuadsFrameFlow,
    jelly.LOGICAL_STREAM_TYPE_GRAPHS: GraphsFrameFlow,
    jelly.LOGICAL_STREAM_TYPE_DATASETS: DatasetsFrameFlow,
}


def flow_for_type(logical_type: jelly.LogicalStreamType) -> type[FrameFlow]:
    """
    Return flow based on logical type requested.

    Note: uses base logical type for subtypes (i.e., SUBJECT_GRAPHS uses
        the same flow as its base type GRAPHS).

    Args:
        logical_type (jelly.LogicalStreamType): logical type requested.

    Raises:
        NotImplementedError: if (base) logical stream type is not supported.

    Returns:
        type[FrameFlow]: FrameFlow for respective logical type.

    """
    try:
        base_logical_type_value = logical_type % 10
        base_name = jelly.LogicalStreamType.Name(base_logical_type_value)
        return FLOW_DISPATCH[getattr(jelly.LogicalStreamType, base_name)]
    except KeyError:
        msg = (
            "unsupported logical stream type: "
            f"{jelly.LogicalStreamType.Name(logical_type)}"
        )
        raise NotImplementedError(msg) from None
