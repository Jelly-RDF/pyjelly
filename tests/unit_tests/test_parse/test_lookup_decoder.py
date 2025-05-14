import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyjelly.errors import JellyAssertionError
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
