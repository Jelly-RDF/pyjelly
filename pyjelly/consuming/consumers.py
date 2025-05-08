from __future__ import annotations

from typing import ClassVar

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError


def consumer_from_jelly_type(
    jelly_type: jelly.LogicalStreamType,
) -> type[StreamConsumer]:
    """Return the StreamConsumer class corresponding to the given jelly type."""
    try:
        return StreamConsumer.registry[jelly_type]
    except KeyError:
        msg = f"unknown stream logical type: {jelly_type}"
        raise JellyConformanceError(msg) from None


class StreamConsumer:
    registry: ClassVar[dict[jelly.LogicalStreamType, type[StreamConsumer]]] = {}

    jelly_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED

    def __init_subclass__(cls) -> None:
        if cls.jelly_type is not jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED:
            cls.registry[cls.jelly_type] = cls


class FlatTriplesFlow(StreamConsumer):
    jelly_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES


class FlatQuadsFlow(StreamConsumer):
    jelly_type = jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
