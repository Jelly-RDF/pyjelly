import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyjelly.errors import JellyConformanceError
from pyjelly.options import LookupPreset


@given(st.integers(min_value=0, max_value=7))
def test_name_encoder_fails_with_size_lt_8(invalid_size: int) -> None:
    # max_name_table_size (9) - maximum size of the name lookup. This field is
    # REQUIRED and MUST be set to a value greater than or equal to 8. The size
    # of the lookup MUST NOT exceed the value of this field.
    with pytest.raises(JellyConformanceError, match="at least 8"):
        LookupPreset(
            max_names=invalid_size,
            max_prefixes=0,
            max_datatypes=0,
        )
