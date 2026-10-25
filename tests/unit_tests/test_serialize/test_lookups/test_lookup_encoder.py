import importlib
from pathlib import Path
from unittest.mock import patch

import pytest
from inline_snapshot import snapshot
from pytest_subtests import SubTests

from pyjelly.serialize.lookup import Lookup, LookupEncoder


class DummyLookupEncoder(LookupEncoder):
    def __init__(
        self, *, lookup_size: int | None = None, lookup: Lookup | None = None
    ) -> None:
        if lookup is None:
            super().__init__(lookup_size=lookup_size or 0)
        else:
            self.lookup = lookup

        self.last_assigned_index = 0
        self.last_reused_index = 0


lookup_mod = importlib.import_module("pyjelly.serialize.lookup")
file_path = Path(getattr(lookup_mod, "__file__", ""))
IS_COMPILED = file_path.suffix.lower() in {".so", ".pyd"}


def test_encode_entry_index() -> None:
    encoder = DummyLookupEncoder(lookup_size=4)

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


@pytest.mark.skipif(IS_COMPILED, reason="compiled extension; patching wont work here")
def test_last_assigned_index() -> None:
    encoder = DummyLookupEncoder(lookup_size=1)

    assert encoder.last_assigned_index == 0

    with patch.object(encoder.lookup, "insert", return_value=7):
        encoder.encode_entry_index("foo")

        assert encoder.last_assigned_index == 7


def test_last_reused_index() -> None:
    encoder = DummyLookupEncoder(lookup_size=3)

    assert encoder.last_assigned_index == 0

    encoder.encode_entry_index("foo")

    passthrough = 0
    encoder.lookup.data["foo"] = passthrough
    encoder.encode_term_index("foo")

    assert encoder.last_reused_index is passthrough


def test_encode_term_index() -> None:
    encoder = DummyLookupEncoder(lookup_size=5)

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


def test_name_index_to_write() -> None:
    encoder = DummyLookupEncoder(lookup_size=3)
    # 0 MUST be interpreted as the previous name index + 1
    assert encoder.name_index_to_write(1) == 0
    assert encoder.name_index_to_write(2) == 0
    assert encoder.name_index_to_write(2) == 2
    assert encoder.name_index_to_write(1) == 1
    assert encoder.name_index_to_write(2) == 0


def test_prefix_index_to_write() -> None:
    encoder = DummyLookupEncoder(lookup_size=3)
    # An empty prefix before any other is written as 0
    assert encoder.prefix_index_to_write(1, empty=True) == 0
    assert encoder.prefix_index_to_write(1, empty=False) == 1
    # 0 MUST be interpreted as the same prefix as in the previous IRI
    assert encoder.prefix_index_to_write(1, empty=False) == 0
    assert encoder.prefix_index_to_write(2, empty=False) == 2
    assert encoder.prefix_index_to_write(2, empty=False) == 0
    # After another prefix, an empty one needs its own entry
    assert encoder.prefix_index_to_write(3, empty=True) == 3


@pytest.mark.skipif(IS_COMPILED, reason="compiled extension; patching wont work here")
def test_encode_datatype_term_index(subtests: SubTests) -> None:
    with subtests.test("lookup size = 3 encodes correctly"):
        encoder = DummyLookupEncoder(lookup_size=3)
        # Test all cases are delegated to previously tested encode_term_index
        passthrough = object()

        with patch.object(encoder, "encode_term_index", return_value=passthrough):
            assert encoder.encode_datatype_term_index("foo") is passthrough

    with subtests.test("lookup size = 0 always encodes 0"):
        encoder = DummyLookupEncoder(lookup_size=0)
        assert encoder.encode_datatype_term_index("foo") == 0
        assert encoder.encode_datatype_term_index("bar") == 0

        # If the [max_datatype_table_size] field is set to 0, the datatype lookup
        # MUST NOT be used in the stream
        with patch.object(encoder, "encode_term_index") as mock:
            encoder.encode_datatype_term_index("foo")
            mock.assert_not_called()
