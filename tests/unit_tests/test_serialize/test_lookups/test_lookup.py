import pytest
from inline_snapshot import snapshot

from pyjelly.errors import JellyConformanceError
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


def test_evicts_in_index_order_skipping_recently_used() -> None:
    lookup = Lookup(4)  # an entry is cold if not used in the last 2 uses
    for key in "abcd":
        lookup.insert(key)  # indices 1-4

    # "a" (index 1) is next in order, but was just used: the least recently used
    # one is taken instead
    assert lookup.use("a") == 1
    assert lookup.insert("e") == 2
    # Then eviction goes on in order from there
    assert lookup.insert("f") == 3
    assert lookup.insert("g") == 4
    assert lookup.data == {"a": 1, "e": 2, "f": 3, "g": 4}


def test_does_not_evict_entries_of_the_current_statement() -> None:
    lookup = Lookup(2)
    lookup.insert("a")
    lookup.insert("b")

    lookup.new_epoch()
    lookup.use("a")  # pins "a"
    assert lookup.insert("c") == 2  # evicts "b", not "a"
    assert "a" in lookup.data

    # Both entries are pinned now, so there's nothing to evict
    with pytest.raises(JellyConformanceError, match="too small"):
        lookup.insert("d")

    # In the next statement, they can be evicted again
    lookup.new_epoch()
    assert lookup.insert("d") in (1, 2)


def test_use_marks_and_returns_index() -> None:
    lookup = Lookup(2)
    assert lookup.use("a") == 0
    lookup.insert("a")
    assert lookup.use("a") == 1


def test_lookup_repr() -> None:
    lk = Lookup(1)
    lk.insert("a")
    assert str(lk) == f"Lookup(max_size={lk.max_size!r}, data={lk.data!r})"
