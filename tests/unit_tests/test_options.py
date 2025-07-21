from __future__ import annotations

from unittest.mock import Mock

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError
from pyjelly.options import StreamParameters, StreamTypes
from pyjelly.parse.decode import options_from_frame


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
    mock_frame = Mock()
    mock_row = Mock()
    mock_options = Mock()

    defaults = {
        "physical_type": jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        "logical_type": jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
        "max_name_table_size": 1000,
        "max_prefix_table_size": 100,
        "max_datatype_table_size": 100,
        "stream_name": "",
        "generalized_statements": bool(generalized_statements),
        "rdf_star": bool(rdf_star),
        "version": 1,
    }

    for key, value in defaults.items():
        setattr(mock_options, key, value)

    mock_row.options = mock_options
    mock_frame.rows = [mock_row]

    result = options_from_frame(mock_frame, delimited=True)

    assert result.params.generalized_statements == defaults["generalized_statements"]
    assert result.params.rdf_star == defaults["rdf_star"]


@pytest.mark.parametrize(
    ("generalized_statements", "rdf_star"), [(0, 0), (0, 1), (1, 0), (1, 1)]
)
def test_stream_parameters(generalized_statements: int, rdf_star: int) -> None:
    mock_options = Mock()

    defaults = {
        "physical_type": jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        "logical_type": jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
        "max_name_table_size": 1000,
        "max_prefix_table_size": 100,
        "max_datatype_table_size": 100,
        "stream_name": "",
        "generalized_statements": bool(generalized_statements),
        "rdf_star": bool(rdf_star),
        "version": 1,
    }

    for key, value in defaults.items():
        setattr(mock_options, key, value)

    params = StreamParameters(
        stream_name=mock_options.stream_name,
        generalized_statements=mock_options.generalized_statements,
        rdf_star=mock_options.rdf_star,
        version=mock_options.version,
        delimited=True,
    )
    assert params.generalized_statements == mock_options.generalized_statements
    assert params.rdf_star == mock_options.rdf_star
