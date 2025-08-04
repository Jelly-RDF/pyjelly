from unittest.mock import patch

import pytest
from inline_snapshot import snapshot
from pytest_subtests import SubTests

from pyjelly.serialize.lookup import LookupEncoder


def test_encode_entry_index() -> None:
    encoder = LookupEncoder(lookup_size=4)

    entry_index = encoder.encode_entry_index("foo")
    assert encoder.lookup.data["foo"] == 1
    # If 0 appears in the first IRI of the stream it MUST be interpreted as 1
    assert entry_index == 0

    entry_index = encoder.encode_entry_index("")
    assert encoder.lookup.data[""] == 2
    # default value of 0 MUST be interpreted as previous_name_id + 1
    assert entry_index == 0

    entry_index = encoder.encode_entry_index("bar")
    assert encoder.lookup.data["bar"] == 3
    assert entry_index == 0

    encoder.last_assigned_index = 10
    entry_index = encoder.encode_entry_index("baz")
    # default value behavior works only if value equals previous + 1
    assert entry_index == snapshot(4)


def test_last_assigned_index() -> None:
    encoder = LookupEncoder(lookup_size=1)

    assert encoder.last_assigned_index == 0

    passthrough = object()

    with patch.object(encoder.lookup, "insert", return_value=passthrough):
        encoder.encode_entry_index("foo")

        assert encoder.last_assigned_index is passthrough


def test_last_reused_index() -> None:
    encoder = LookupEncoder(lookup_size=3)

    assert encoder.last_assigned_index == 0

    encoder.encode_entry_index("foo")

    passthrough = object()
    encoder.lookup.data["foo"] = passthrough  # type: ignore[assignment]
    encoder.encode_term_index("foo")

    assert encoder.last_reused_index is passthrough


def test_encode_term_index() -> None:
    encoder = LookupEncoder(lookup_size=5)

    encoder.encode_entry_index("foo")
    encoder.encode_entry_index("bar")
    encoder.encode_entry_index("biz")
    encoder.encode_entry_index("baz")
    encoder.encode_entry_index("qux")

    assert encoder.encode_term_index("qux") == 5
    assert encoder.encode_term_index("bar") == 2
    assert encoder.encode_term_index("baz") == 4
    assert encoder.encode_term_index("foo") == 1
    assert encoder.encode_term_index("biz") == 3


def test_encode_name_term_index(subtests: SubTests) -> None:
    with subtests.test("lookup size = 3 encodes correctly"):
        encoder = LookupEncoder(lookup_size=3)

        encoder.encode_entry_index("foo")
        encoder.encode_entry_index("bar")

        # Default value of 0 MUST be interpreted as previous_name_id + 1
        previous_id = encoder.last_reused_index
        assert encoder.lookup.data["foo"] == previous_id + 1
        assert encoder.encode_name_term_index("foo") == 0

        previous_id = encoder.last_reused_index
        assert encoder.lookup.data["bar"] == previous_id + 1
        assert encoder.encode_name_term_index("bar") == 0

        encoder.encode_entry_index("baz")
        previous_id = encoder.last_reused_index
        assert encoder.lookup.data["baz"] == previous_id + 1
        assert encoder.encode_name_term_index("baz") == 0

        # Test all non-specialized cases are delegated to previously tested
        # encode_term_index
        passthrough = object()

        with patch.object(encoder, "encode_term_index", return_value=passthrough):
            assert encoder.encode_name_term_index("baz") is passthrough

    # [max_name_table_size] (...) MUST be set to a value greater than or equal to 8.
    with subtests.test("lookup size = 0 fails"):
        encoder = LookupEncoder(lookup_size=0)

        with pytest.raises(KeyError):
            encoder.encode_name_term_index("foo")

        with pytest.raises(KeyError):
            encoder.encode_name_term_index("bar")


def test_encode_prefix_term_index(subtests: SubTests) -> None:
    with subtests.test("empty prefix encodes 0 at first"):
        encoder = LookupEncoder(lookup_size=3)
        assert encoder.encode_prefix_term_index("") == 0
        assert not encoder.lookup.data

    with subtests.test("empty prefix encoded after non-empty prefix"):
        encoder = LookupEncoder(lookup_size=3)
        encoder.encode_entry_index("foo")
        encoder.encode_entry_index("")

        assert encoder.encode_prefix_term_index("foo") == 1
        assert encoder.encode_prefix_term_index("") == 2

    with subtests.test("lookup size = 3 encodes correctly"):
        encoder = LookupEncoder(lookup_size=3)

        encoder.encode_entry_index("foo")
        encoder.encode_entry_index("bar")

        # The default value of 0 MUST be interpreted as the same value
        # as in the last explicitly specified (non-zero) prefix identifier
        assert encoder.encode_prefix_term_index("foo") == 1
        assert encoder.encode_prefix_term_index("foo") == 0
        assert encoder.encode_prefix_term_index("bar") == 2
        assert encoder.encode_prefix_term_index("bar") == 0

        encoder.encode_entry_index("baz")
        assert encoder.encode_prefix_term_index("baz") == 3
        assert encoder.encode_prefix_term_index("baz") == 0

        encoder.encode_entry_index("qux")

        # Test all non-specialized cases are delegated to previously tested
        # encode_term_index
        passthrough = object()

        with patch.object(encoder, "encode_term_index", return_value=passthrough):
            assert encoder.encode_prefix_term_index("baz") is passthrough

    with subtests.test("lookup size = 0 always encodes 0"):
        encoder = LookupEncoder(lookup_size=0)
        assert encoder.encode_prefix_term_index("foo") == 0
        assert encoder.encode_prefix_term_index("bar") == 0

        # If the [max_prefix_table_size] field is set to 0, the prefix lookup
        # MUST NOT be used in the stream
        with patch.object(encoder, "encode_term_index") as mock:
            mock.assert_not_called()


def test_encode_datatype_term_index(subtests: SubTests) -> None:
    with subtests.test("lookup size = 3 encodes correctly"):
        encoder = LookupEncoder(lookup_size=3)
        # Test all cases are delegated to previously tested encode_term_index
        passthrough = object()

        with patch.object(encoder, "encode_term_index", return_value=passthrough):
            assert encoder.encode_datatype_term_index("foo") is passthrough

    with subtests.test("lookup size = 0 always encodes 0"):
        encoder = LookupEncoder(lookup_size=0)
        assert encoder.encode_datatype_term_index("foo") == 0
        assert encoder.encode_datatype_term_index("bar") == 0

        # If the [max_datatype_table_size] field is set to 0, the datatype lookup
        # MUST NOT be used in the stream
        with patch.object(encoder, "encode_term_index") as mock:
            encoder.encode_datatype_term_index("foo")
            mock.assert_not_called()
