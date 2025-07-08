from __future__ import annotations

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError
from pyjelly.options import StreamParameters
from pyjelly.serialize.encode import TermEncoder
from pyjelly.serialize.flows import (
    DatasetsFrameFlow,
    FlatQuadsFrameFlow,
    FlatTriplesFrameFlow,
    FrameFlow,
    GraphsFrameFlow,
    ManualFrameFlow,
    flow_for_type,
)
from pyjelly.serialize.streams import (
    GraphStream,
    QuadStream,
    SerializerOptions,
    Stream,
    TripleStream,
)


def test_flat_triples_inference_delimited() -> None:
    stream = TripleStream(encoder=TermEncoder(), options=None)
    assert isinstance(stream.flow, FlatTriplesFrameFlow)
    assert stream.flow.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES


@pytest.mark.parametrize("stream_class", [QuadStream, GraphStream])
def test_flat_quads_inference_delimited(
    stream_class: type[GraphStream | QuadStream],
) -> None:
    stream = stream_class(encoder=TermEncoder(), options=None)
    assert isinstance(stream.flow, FlatQuadsFrameFlow)
    assert stream.flow.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS


@pytest.mark.parametrize(
    (
        "logical_type",
        "flow_type",
    ),
    [
        (jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES, FlatTriplesFrameFlow),
        (jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS, FlatQuadsFrameFlow),
        (jelly.LOGICAL_STREAM_TYPE_GRAPHS, GraphsFrameFlow),
        (jelly.LOGICAL_STREAM_TYPE_DATASETS, DatasetsFrameFlow),
        (jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS, GraphsFrameFlow),
        (jelly.LOGICAL_STREAM_TYPE_NAMED_GRAPHS, DatasetsFrameFlow),
        (jelly.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS, DatasetsFrameFlow),
    ],
)
def test_flow_base_logical_type(
    logical_type: jelly.LogicalStreamType, flow_type: type[FrameFlow]
) -> None:
    assert flow_for_type(logical_type) is flow_type


@pytest.mark.parametrize(
    ("stream_class", "logical_type"),
    [
        (TripleStream, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (TripleStream, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (QuadStream, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (QuadStream, jelly.LOGICAL_STREAM_TYPE_DATASETS),
        (GraphStream, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (GraphStream, jelly.LOGICAL_STREAM_TYPE_DATASETS),
    ],
)
def test_flow_logical_type_nondelimited(
    stream_class: type[Stream],
    logical_type: jelly.LogicalStreamType,
) -> None:
    stream = stream_class(
        encoder=TermEncoder(),
        options=SerializerOptions(
            logical_type=logical_type,
            params=StreamParameters(delimited=False),
        ),
    )
    assert isinstance(stream.flow, ManualFrameFlow)
    assert stream.flow.logical_type == logical_type


@pytest.mark.parametrize(
    ("stream_class", "logical_type"),
    [
        (TripleStream, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (QuadStream, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (GraphStream, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
    ],
)
def test_wrong_inference(
    stream_class: type[Stream], logical_type: jelly.LogicalStreamType
) -> None:
    # A couple obvious examples of incompatible combinations
    with pytest.raises(JellyAssertionError):
        stream_class(
            encoder=TermEncoder(),
            options=SerializerOptions(
                logical_type=logical_type,
                params=StreamParameters(delimited=False),
            ),
        )
