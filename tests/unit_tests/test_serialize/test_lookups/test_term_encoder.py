import pytest
from inline_snapshot import snapshot
from pytest_subtests import SubTests

from pyjelly.errors import JellyConformanceError
from pyjelly.options import LookupPreset
from pyjelly.serialize.encode import TermEncoder


def test_encode_literal_fails_with_disabled_datatype_lookup() -> None:
    encoder = TermEncoder(
        lookup_preset=LookupPreset(
            max_names=8,
            max_prefixes=8,
            max_datatypes=0,
        )
    )
    with pytest.raises(
        JellyConformanceError,
        match="datatype lookup cannot be used if disabled",
    ):
        encoder.encode_literal(
            lex="42",
            datatype="http://www.w3.org/2001/XMLSchema#integer",
        )


def test_encode_literal_ok_with_string_and_langtag(subtests: SubTests) -> None:
    encoder = TermEncoder(
        lookup_preset=LookupPreset(
            max_names=8,
            max_prefixes=8,
            max_datatypes=0,
        )
    )

    with subtests.test("xsd:string is skipped by datatype lookup → no error"):
        _, literal = encoder.encode_literal(
            lex="foo",
            datatype="http://www.w3.org/2001/XMLSchema#string",
        )
        assert literal.lex == snapshot("foo")
        assert literal.datatype == snapshot(0)
        assert literal.langtag == snapshot("")

    with subtests.test("no datatype or langtag"):
        _, literal = encoder.encode_literal(lex="bar")
        assert literal.lex == snapshot("bar")
        assert literal.datatype == snapshot(0)
        assert literal.langtag == snapshot("")

    with subtests.test("no datatype or langtag"):
        _, literal = encoder.encode_literal(lex="baz", language="en")
        assert literal.lex == snapshot("baz")
        assert literal.langtag == snapshot("en")
        assert literal.datatype == snapshot(0)
