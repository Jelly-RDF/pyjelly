import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyjelly.errors import JellyAssertionError, JellyConformanceError
from pyjelly.options import MAX_LOOKUP_SIZE
from pyjelly.parse.lookup import LookupDecoder


@given(st.integers(min_value=1, max_value=MAX_LOOKUP_SIZE))
def test_lookup_size_ok(size: int) -> None:
    LookupDecoder(lookup_size=size)


@given(st.integers(min_value=MAX_LOOKUP_SIZE + 1))
def test_max_lookup_size_exceeded(size: int) -> None:
    with pytest.raises(JellyAssertionError) as excinfo:
        LookupDecoder(lookup_size=size)
    assert str(excinfo.value) == f"lookup size must be less than {MAX_LOOKUP_SIZE}"


def test_decode_zero_error() -> None:
    dec = LookupDecoder(lookup_size=1)
    dec.last_reused_index = -1
    with pytest.raises(JellyConformanceError):
        dec.decode_name_term_index(0)


def test_datatype_index_zero_error() -> None:
    decoder = LookupDecoder(lookup_size=0)
    with pytest.raises(JellyConformanceError) as excinfo:
        decoder.decode_datatype_term_index(0)
    assert str(excinfo.value) == "0 is not a valid datatype term index"


def test_at_invalid_index() -> None:
    decoder = LookupDecoder(lookup_size=4)
    with pytest.raises(IndexError) as excinfo:
        decoder.at(2)
    assert "invalid resolved index 2" in str(excinfo.value)
    assert decoder.last_reused_index == 2
