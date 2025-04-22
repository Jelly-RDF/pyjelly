import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyjelly.options import StreamOptions


@given(st.integers(min_value=0, max_value=7))
def test_name_encoder_fails_with_size_lt_8(invalid_size: int) -> None:
    # max_name_table_size (9) - maximum size of the name lookup. This field is
    # REQUIRED and MUST be set to a value greater than or equal to 8. The size
    # of the lookup MUST NOT exceed the value of this field.
    with pytest.raises(AssertionError, match="at least 8"):
        StreamOptions(
            name_lookup_size=invalid_size,
            prefix_lookup_size=0,
            datatype_lookup_size=0,
            version=1,
        )
