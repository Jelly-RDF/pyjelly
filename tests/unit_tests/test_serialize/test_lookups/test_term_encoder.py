import pytest
from inline_snapshot import snapshot
from pytest_subtests import SubTests

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import LookupPreset
from pyjelly.serialize.encode import (
    Slot,
    TermEncoder,
    encode_namespace_declaration,
)


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
            literal=jelly.RdfTriple().s_literal,
        )


def test_encode_any_raises_not_implemented() -> None:
    encoder = TermEncoder()
    with pytest.raises(NotImplementedError) as exc:
        encoder.encode_spo(123, Slot.subject, statement=jelly.RdfTriple())
    assert "unsupported term type: <class 'int'>" in str(exc.value)


def test_encode_literal_ok_with_string_and_langtag(subtests: SubTests) -> None:
    encoder = TermEncoder(
        lookup_preset=LookupPreset(
            max_names=8,
            max_prefixes=8,
            max_datatypes=0,
        )
    )

    with subtests.test("xsd:string is skipped by datatype lookup → no error"):
        statement = jelly.RdfTriple()
        _ = encoder.encode_literal(
            lex="foo",
            datatype="http://www.w3.org/2001/XMLSchema#string",
            literal=statement.s_literal,
        )
        assert statement.s_literal.lex == snapshot("foo")
        assert statement.s_literal.datatype == snapshot(0)
        assert statement.s_literal.langtag == snapshot("")

    with subtests.test("no datatype or langtag"):
        statement = jelly.RdfTriple()
        _ = encoder.encode_literal(
            lex="bar",
            literal=statement.s_literal,
        )
        assert statement.s_literal.lex == snapshot("bar")
        assert statement.s_literal.datatype == snapshot(0)
        assert statement.s_literal.langtag == snapshot("")

    with subtests.test("no datatype but langtag"):
        statement = jelly.RdfTriple()
        _ = encoder.encode_literal(
            lex="baz",
            language="en",
            literal=statement.s_literal,
        )
        assert statement.s_literal.lex == snapshot("baz")
        assert statement.s_literal.langtag == snapshot("en")
        assert statement.s_literal.datatype == snapshot(0)


def test_encode_namespace_declaration() -> None:
    encoder = TermEncoder()
    rows = encode_namespace_declaration("ex", "http://example.org/A", encoder)

    assert isinstance(rows[-1].namespace, jelly.RdfNamespaceDeclaration)

    assert any(r.prefix for r in rows[:-1])
    assert any(r.name for r in rows[:-1])
