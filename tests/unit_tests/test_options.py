import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError
from pyjelly.options import validate_type_compatibility


@pytest.mark.parametrize(
    ("physical_type", "logical_type"),
    [
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS),
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
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
    ],
)
def test_validate_type_compatibility_ok(
    physical_type: jelly.PhysicalStreamType,
    logical_type: jelly.LogicalStreamType,
) -> None:
    validate_type_compatibility(physical_type, logical_type)


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
        (jelly.PHYSICAL_STREAM_TYPE_TRIPLES, jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_QUADS, jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (jelly.PHYSICAL_STREAM_TYPE_GRAPHS, jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS),
        (
            jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        ),
        (jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED, jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        (
            jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS,
        ),
    ],
)
def test_validate_type_compatibility_error(
    physical_type: jelly.PhysicalStreamType,
    logical_type: jelly.LogicalStreamType,
) -> None:
    with pytest.raises(JellyAssertionError):
        validate_type_compatibility(physical_type, logical_type)
