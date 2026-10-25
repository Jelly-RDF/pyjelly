from __future__ import annotations

import io

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError, JellyConformanceError
from pyjelly.options import (
    MAX_VERSION,
    MIN_NAME_LOOKUP_SIZE,
    VERSION_1_0,
    LookupPreset,
    StreamParameters,
    StreamTypes,
)
from pyjelly.parse.decode import options_from_frame
from pyjelly.parse.ioutils import get_options_and_frames


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


@pytest.mark.parametrize(
    ("generalized_statements", "rdf_star"), [(0, 0), (0, 1), (1, 0), (1, 1)]
)
def test_optional_fields(generalized_statements: int, rdf_star: int) -> None:
    options = jelly.RdfStreamOptions(
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        max_name_table_size=1000,
        max_prefix_table_size=100,
        max_datatype_table_size=100,
        generalized_statements=bool(generalized_statements),
        rdf_star=bool(rdf_star),
        version=1,
    )
    frame = jelly.RdfStreamFrame(rows=[jelly.RdfStreamRow(options=options)])

    result = options_from_frame(frame, delimited=True)

    assert result.params.generalized_statements == bool(generalized_statements)
    assert result.params.rdf_star == bool(rdf_star)


def test_options_from_frame_needs_options_first() -> None:
    frame = jelly.RdfStreamFrame(
        rows=[jelly.RdfStreamRow(name=jelly.RdfNameEntry(value="x"))]
    )
    with pytest.raises(JellyConformanceError, match="does not start with"):
        options_from_frame(frame, delimited=True)


def test_lookup_preset_validation() -> None:
    with pytest.raises(JellyConformanceError):
        LookupPreset(max_names=MIN_NAME_LOOKUP_SIZE - 1)
    p = LookupPreset.small()
    assert (p.max_names, p.max_prefixes, p.max_datatypes) == (128, 32, 32)


def test_stream_parameters_version() -> None:
    assert StreamParameters().version == MAX_VERSION
    assert StreamParameters(version=VERSION_1_0).version == VERSION_1_0


@pytest.mark.parametrize("version", [0, MAX_VERSION + 1])
def test_stream_parameters_invalid_version(version: int) -> None:
    with pytest.raises(JellyConformanceError, match="unsupported protocol version"):
        StreamParameters(version=version)


def test_namespace_declarations_need_version_1_1() -> None:
    with pytest.raises(JellyConformanceError, match="need protocol version 2"):
        StreamParameters(version=VERSION_1_0, namespace_declarations=True)


@pytest.mark.parametrize("data", [b"\x00\x00\x00", b""])
def test_get_options_and_frames_empty_stream(data: bytes) -> None:
    # A stream with no stream options is an empty graph
    options, frames = get_options_and_frames(io.BytesIO(data))
    assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    assert all(not frame.rows for frame in frames)


def test_get_options_and_frames_non_delimited_success_only_return() -> None:
    opts_msg = jelly.RdfStreamOptions(
        stream_name="test",
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        generalized_statements=False,
        rdf_star=False,
        max_name_table_size=8,
        max_prefix_table_size=8,
        max_datatype_table_size=8,
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        version=1,
    )
    frame = jelly.RdfStreamFrame(rows=[jelly.RdfStreamRow(options=opts_msg)])
    data = frame.SerializeToString(deterministic=True)

    options, frames_iter = get_options_and_frames(io.BytesIO(data))

    assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES

    frames = list(frames_iter)
    assert len(frames) == 1
    assert isinstance(frames[0], jelly.RdfStreamFrame)
    assert frames[0].rows[0].options == opts_msg
