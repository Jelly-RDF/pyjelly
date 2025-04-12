from pyjelly._serializing.lookups import (
    DatatypeLookupEncoder,
    Lookup,
    NameLookupEncoder,
    PrefixLookupEncoder,
)


def test_prefix_lookup_entry_and_term() -> None:
    lookup = PrefixLookupEncoder(lookup=Lookup(size=3))

    # new prefix → entry id assigned
    assert lookup.index_for_entry("http://a/") == 0
    # reused prefix → no entry needed
    assert lookup.index_for_entry("http://a/") is None
    # term usage → returns ID (first access)
    assert lookup.index_for_term("http://a/") == 1
    # second access (same term) → returns None (delta encoding)
    assert lookup.index_for_term("http://a/") is None
    # second new prefix → assigned
    assert lookup.index_for_entry("http://b/") == 0
    assert lookup.index_for_term("http://b/") == 2

    # empty prefix → sentinel
    assert lookup.index_for_entry("") == 0


def test_name_lookup_entry_and_term() -> None:
    lookup = NameLookupEncoder(lookup=Lookup(size=2))

    assert lookup.index_for_entry("foo") == 0
    assert lookup.index_for_entry("foo") is None
    assert lookup.index_for_term("foo") == 0
    assert lookup.index_for_term("foo") == 1
    assert lookup.index_for_entry("bar") == 0
    assert lookup.index_for_term("bar") == 0

    # trigger LRU eviction
    assert lookup.index_for_entry("baz") == 1  # reuses evicted ID


def test_datatype_lookup_skip_behavior() -> None:
    dt = DatatypeLookupEncoder(lookup=Lookup(size=3))
    xsd_string = "http://www.w3.org/2001/XMLSchema#string"
    xsd_int = "http://www.w3.org/2001/XMLSchema#int"

    # xsd:string → should be skipped
    assert dt.index_for_entry(xsd_string) is None
    assert dt.index_for_term(xsd_string) is None

    # normal datatype → assigned
    assert dt.index_for_entry(xsd_int) == 0
    assert dt.index_for_term(xsd_int) == 1
    assert dt.index_for_term(xsd_int) == 1  # no delta encoding for datatype

    # LRU eviction
    dt.index_for_entry("http://a")
    dt.index_for_entry("http://b")
    assert dt.index_for_entry("http://c") == 1  # reuses evicted ID
