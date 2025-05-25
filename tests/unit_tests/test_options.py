from __future__ import annotations

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError
from pyjelly.options import StreamTypes


@pytest.mark.parametrize(
    ("physical_type", "logical_type"),
    VALID_STREAM_TYPE_COMBINATIONS := [
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_DATASETS),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_NAMED_GRAPHS),
        (
            jelly.PHYSICAL_STREAM_TYPE_QUADS,
            jelly.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS,
        ),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_DATASETS),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_NAMED_GRAPHS),
        (
            jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
            jelly.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS,
        ),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
        (
            jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        ),
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (
            jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS,
        ),
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_DATASETS),
        (
            jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_NAMED_GRAPHS,
        ),
        (
            jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS,
        ),
    ],
)
def test_stream_types_ok(
    physical_type: jelly.PhysicalStreamType,
    logical_type: jelly.LogicalStreamType,
) -> None:
    StreamTypes(physical_type=physical_type, logical_type=logical_type)


@pytest.mark.parametrize(
    ("physical_type", "logical_type"),
    [
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_DATASETS),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_NAMED_GRAPHS),
        (
            jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            jelly.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS,
        ),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS),
    ],
)
def test_stream_types_error(
    physical_type: jelly.PhysicalStreamType,
    logical_type: jelly.LogicalStreamType,
) -> None:
    with pytest.raises(JellyAssertionError):
        StreamTypes(physical_type=physical_type, logical_type=logical_type)
