import pytest
from inline_snapshot import snapshot

from pyjelly.serialize.lookup import Lookup


def current_size(lookup: Lookup) -> int:
    return len(lookup.data)


def test_maintains_size_0() -> None:
    lookup = Lookup(0)
    with pytest.raises(IndexError, match="cannot insert"):
        assert lookup.insert("foo") is None
    assert current_size(lookup) == 0


@pytest.mark.parametrize("max_size", [1, 2, 10, 11, 50, 1190])
def test_maintains_size(max_size: int) -> None:
    lookup = Lookup(max_size)

    # Sequential path
    for i in range(max_size):
        lookup.insert(f"key{i}")
        # It is not important to use exactly as much space as necessary
        # at this point of time. The critical requirement is to never
        # exceed the maximum size
        assert current_size(lookup) <= max_size

    # Evicting path
    for i in range(max_size, max_size * 2):
        lookup.insert(f"key{i}")
        # In the evicting path, it is never possible for the lookup
        # to be at less than its full capacity, because pre-existing entries
        # never expire
        assert current_size(lookup) == max_size


def test_insert_sequential() -> None:
    lookup = Lookup(3)

    assert lookup.insert("key1") == snapshot(1)
    assert lookup.insert("key2") == snapshot(2)
    assert lookup.insert("key3") == snapshot(3)


def test_insert_evicting() -> None:
    lookup = Lookup(3)

    lookup.insert("key1")
    lookup.insert("key2")
    lookup.insert("key3")

    assert lookup.insert("key4") == snapshot(1)
    assert lookup.insert("key5") == snapshot(2)
    assert lookup.insert("key6") == snapshot(3)
    assert lookup.insert("key7") == snapshot(1)
    assert lookup.insert("key8") == snapshot(2)
    assert lookup.insert("key9") == snapshot(3)


def test_insert_asserts_fresh_key() -> None:
    lookup = Lookup(1)
    lookup.insert("foo")
    with pytest.raises(AssertionError, match="key 'foo' already present"):
        lookup.insert("foo")


def test_make_last_to_evict() -> None:
    lookup = Lookup(3)
    a = lookup.insert("a")
    # evict order: a

    b = lookup.insert("b")
    # evict order: a, b

    lookup.make_last_to_evict("a")
    # b, a

    c = lookup.insert("c")
    # b, a, c

    d = lookup.insert("d")  # evicts b
    # a, c, d

    assert d == b

    e = lookup.insert("e")  # evicts a
    # c, d, e

    assert e == a

    f = lookup.insert("f")  # evicts c
    # d, e, f

    assert f == c

    lookup.make_last_to_evict("e")
    # d, f, e

    g = lookup.insert("g")  # evicts d
    # f, e, g

    assert g == d

    h = lookup.insert("h")  # evicts f
    # e, g, h

    assert h == f

    i = lookup.insert("i")  # evicts e
    # g, h, i

    assert i == e


def test_make_last_to_evict_for_existing_key_raises() -> None:
    lookup = Lookup(1)
    with pytest.raises(KeyError, match="key1"):
        lookup.make_last_to_evict("key1")
