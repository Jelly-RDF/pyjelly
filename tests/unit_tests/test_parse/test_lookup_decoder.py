import pytest

from pyjelly.errors import JellyConformanceError
from pyjelly.parse.lookup import LookupDecoder


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


def test_assign_entry_outside_lookup() -> None:
    decoder = LookupDecoder(lookup_size=2)
    decoder.assign_entry(2, "b")
    with pytest.raises(JellyConformanceError, match="the lookup has 2 entries"):
        decoder.assign_entry(0, "c")
