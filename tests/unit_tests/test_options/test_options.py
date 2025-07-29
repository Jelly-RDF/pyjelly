import mimetypes

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import (
    MIN_NAME_LOOKUP_SIZE,
    MIN_VERSION,
    MAX_VERSION,
    LookupPreset,
    StreamParameters,
    StreamTypes,
    validate_type_compatibility,
)
import pyjelly.options as op

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

def test_validate_type_unspecified() -> None:
    validate_type_compatibility(
        jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED,
        jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
    )


def test_stream_parameters_version() -> None:
    s1 = StreamParameters(namespace_declarations=False)
    assert s1.version == MIN_VERSION
    s2 = StreamParameters(namespace_declarations=True)
    assert s2.version == 2

def test_stream_options_invalid_version(monkeypatch) -> None:
    # Force MIN_VERSION > MAX_VERSION to trigger validation error
    monkeypatch.setattr(op, 'MIN_VERSION', 10)
    monkeypatch.setattr(op, 'MAX_VERSION', 5)

    with pytest.raises(JellyConformanceError) as excinfo:
        StreamParameters(namespace_declarations=True)

    msg = str(excinfo.value)
    assert 'Version must be between 10 and 5' in msg
