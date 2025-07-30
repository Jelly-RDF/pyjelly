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


def test_lookup_decoder_flow() -> None:
    dec = LookupDecoder(lookup_size=3)

    assert dec.decode_prefix_term_index(0) == ""
    dec.assign_entry(0, "a")
    dec.assign_entry(0, "b")

    assert dec.at(1) == "a"
    assert dec.decode_prefix_term_index(1) == "a"
    assert dec.decode_name_term_index(0) == "b"

    with pytest.raises(JellyConformanceError):
        dec.decode_datatype_term_index(0)
    assert dec.decode_datatype_term_index(2) == "b"

    with pytest.raises(IndexError):
        dec.at(3)


def test_decode_zero_error() -> None:
    dec = LookupDecoder(lookup_size=1)
    dec.last_reused_index = -1
    with pytest.raises(JellyConformanceError):
        dec.decode_name_term_index(0)
