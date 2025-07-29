import pytest

from pyjelly.errors import JellyConformanceError
from pyjelly.parse.lookup import LookupDecoder
from pyjelly.serialize.lookup import Lookup, LookupEncoder


def test_lookup_core_functionality() -> None:
    lk = Lookup(2)
    with pytest.raises(IndexError):
        Lookup(0).insert("x")

    a = lk.insert("a")
    b = lk.insert("b")
    with pytest.raises(AssertionError):
        lk.insert("a")

    lk.make_last_to_evict("a")
    c = lk.insert("c")

    assert (a, b, c) == (1, 2, 2)
    assert list(lk.data.items()) == [("a", 1), ("c", 2)]


def test_lookup_repr() -> None:
    lk = Lookup(1)
    lk.insert("a")
    assert str(lk) == f"Lookup(max_size={lk.max_size!r}, data={lk.data!r})"


def test_encode_entry() -> None:
    enc = LookupEncoder(lookup_size=3)
    assert enc.encode_entry_index("a") == 0
    assert enc.encode_entry_index("b") == 0
    assert enc.encode_entry_index("a") is None
    assert enc.encode_entry_index("c") == 0
    assert enc.encode_entry_index("d") == 2


def test_prefix_term_index_all() -> None:
    enc = LookupEncoder(lookup_size=3)
    assert enc.encode_prefix_term_index("") == 0
    enc.encode_entry_index("a")
    first = enc.encode_prefix_term_index("a")
    second = enc.encode_prefix_term_index("a")
    assert (first, second) == (1, 0)


def test_datatype_term_zero_lookup() -> None:
    enc = LookupEncoder(lookup_size=0)
    assert enc.encode_datatype_term_index("x") == 0


def test_name_term_index_zero_next() -> None:
    enc = LookupEncoder(lookup_size=3)
    enc.encode_entry_index("a")  # id 1
    enc.encode_term_index("a")
    enc.encode_entry_index("b")  # id 2
    assert enc.encode_name_term_index("b") == 0
    enc.encode_entry_index("c")  # id 3
    enc.encode_term_index("c")
    enc.encode_entry_index("x")
    result = enc.encode_name_term_index("c")
    assert result in (1, 2, 3)
    assert result != 0


# def test_lookup_decoder_size_error() -> None:
#     with pytest.raises(JellyAssertionError):
#         LookupDecoder(lookup_size=MAX_LOOKUP_SIZE + 1)


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
