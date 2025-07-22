import mimetypes

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError, JellyConformanceError
from pyjelly.options import (
    MAX_VERSION,
    MIMETYPES,
    MIN_NAME_LOOKUP_SIZE,
    MIN_VERSION,
    LookupPreset,
    register_mimetypes,
    validate_type_compatibility,
)
from pyjelly.serialize.streams import StreamParameters, StreamTypes


def test_register_mimetypes() -> None:
    register_mimetypes()
    assert mimetypes.guess_type("x.jelly")[0] == MIMETYPES[0]


def test_lookup_preset_validation() -> None:
    with pytest.raises(JellyConformanceError):
        LookupPreset(max_names=MIN_NAME_LOOKUP_SIZE - 1)
    p = LookupPreset.small()
    assert (p.max_names, p.max_prefixes, p.max_datatypes) == (128, 32, 32)


def test_stream_types_flat_and_repr() -> None:
    st = StreamTypes(
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
    )
    assert st.flat
    expected = (
        f"StreamTypes("
        f"{jelly.PhysicalStreamType.Name(st.physical_type)}, "
        f"{jelly.LogicalStreamType.Name(st.logical_type)})"
    )
    assert str(st) == expected


def test_stream_types_incompatible_raises() -> None:
    with pytest.raises(JellyAssertionError):
        StreamTypes(
            physical_type=jelly.PHYSICAL_STREAM_TYPE_QUADS,
            logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        )


def test_validate_type_unspecified() -> None:
    validate_type_compatibility(
        jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
        jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
    )


def test_stream_parameters_version() -> None:
    s1 = StreamParameters(namespace_declarations=False)
    assert s1.version == MIN_VERSION
    s2 = StreamParameters(namespace_declarations=True)
    assert s2.version == MAX_VERSION


def test_stream_types_repr_supress() -> None:
    physical_val = 9999
    logical_val = 8888
    st = StreamTypes(physical_type=physical_val, logical_type=logical_val)
    assert str(st) == f"StreamTypes({physical_val}, {logical_val})"
