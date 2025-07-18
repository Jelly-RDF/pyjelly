import unittest
from pathlib import Path

import pytest

from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Triple,
)


class TestGenericStatementSink(unittest.TestCase):
    def setUp(self) -> None:
        self.test_obj = GenericStatementSink("graph_id")
        self.store_content = (
            IRI("http://example.com/s"),
            IRI("http://example.com/p"),
            IRI("http://example.com/o"),
        )
        self.test_obj.add(Triple(*self.store_content))
        self.test_obj.bind("ex", IRI("http://example.com/"))

    def test_namespaces_property(self) -> None:
        namespaces = list(self.test_obj.namespaces)
        assert namespaces[0][0] == "ex"
        assert repr(namespaces[0][1]) == repr(IRI("http://example.com/"))

    def test_identifier_property(self) -> None:
        assert self.test_obj.identifier == "graph_id"

    def test_store_property(self) -> None:
        for statement in self.test_obj.store:
            assert statement == Triple(*self.store_content)

    def test_iterator(self) -> None:
        for statement in self.test_obj:
            assert statement == Triple(*self.store_content)

    def test_len(self) -> None:
        assert len(self.test_obj) == 1


@pytest.mark.parametrize(
    (
        "input_id",
        "expected_repr",
    ),
    [
        ("bn1", "BlankNode(identifier=bn1)"),
        ("929292", "BlankNode(identifier=929292)"),
        ("utf-8~", "BlankNode(identifier=utf-8~)"),
    ],
)
def test_bn_repr(input_id: str, expected_repr: str) -> None:
    bn = BlankNode(input_id)
    assert repr(bn) == expected_repr


@pytest.mark.parametrize(
    (
        "input_id",
        "expected_repr",
    ),
    [
        ("bn1", "_:bn1"),
        ("929292", "_:929292"),
        ("utf-8~", "_:utf-8~"),
    ],
)
def test_bn_str(input_id: str, expected_repr: str) -> None:
    bn = BlankNode(input_id)
    assert str(bn) == expected_repr


@pytest.mark.parametrize(
    (
        "lex",
        "langtag",
        "datatype",
        "expected_str",
    ),
    [
        ("hello", None, None, '"hello"'),
        ("", None, None, '""'),
        ("123", None, None, '"123"'),
        ("true", None, None, '"true"'),
        ("hello", "en", None, '"hello"@en'),
        ("bonjour", "fr", None, '"bonjour"@fr'),
        ("hallo", "de", None, '"hallo"@de'),
        ("hello", "en-US", None, '"hello"@en-US'),
        ("café", "fr", None, '"café"@fr'),
        (
            "123",
            None,
            "http://www.w3.org/2001/XMLSchema#integer",
            '"123"^^<http://www.w3.org/2001/XMLSchema#integer>',
        ),
        (
            "3.14",
            None,
            "http://www.w3.org/2001/XMLSchema#float",
            '"3.14"^^<http://www.w3.org/2001/XMLSchema#float>',
        ),
        (
            "true",
            None,
            "http://www.w3.org/2001/XMLSchema#boolean",
            '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>',
        ),
        (
            "2023-01-01",
            None,
            "http://www.w3.org/2001/XMLSchema#date",
            '"2023-01-01"^^<http://www.w3.org/2001/XMLSchema#date>',
        ),
        (
            "hello",
            None,
            "http://www.w3.org/2001/XMLSchema#string",
            '"hello"^^<http://www.w3.org/2001/XMLSchema#string>',
        ),
        ('He said "hello"', None, None, '"He said "hello""'),
        ("line1\nline2", None, None, '"line1\nline2"'),
        ("tab\there", None, None, '"tab\there"'),
        ("backslash\\here", None, None, '"backslash\\here"'),
        ("résumé", "fr", None, '"résumé"@fr'),
        ("München", "de", None, '"München"@de'),
        ("日本語", "ja", None, '"日本語"@ja'),
        ("🚀", None, None, '"🚀"'),
        (" ", None, None, '" "'),
        ("  whitespace  ", None, None, '"  whitespace  "'),
        ("", "en", None, '""@en'),
        (
            "",
            None,
            "http://www.w3.org/2001/XMLSchema#string",
            '""^^<http://www.w3.org/2001/XMLSchema#string>',
        ),
    ],
)
def test_literal_str(lex: str, langtag: str, datatype: str, expected_str: str) -> None:
    """Test string representation of literals."""
    literal = Literal(lex, langtag=langtag, datatype=datatype)
    assert str(literal) == expected_str


@pytest.mark.parametrize(
    (
        "lex",
        "langtag",
        "datatype",
        "expected_repr",
    ),
    [
        ("hello", None, None, "Literal('hello', langtag=None, datatype=None)"),
        ("", None, None, "Literal('', langtag=None, datatype=None)"),
        ("123", None, None, "Literal('123', langtag=None, datatype=None)"),
        ("hello", "en", None, "Literal('hello', langtag='en', datatype=None)"),
        ("bonjour", "fr", None, "Literal('bonjour', langtag='fr', datatype=None)"),
        ("café", "fr", None, "Literal('café', langtag='fr', datatype=None)"),
        ("hello", "en-US", None, "Literal('hello', langtag='en-US', datatype=None)"),
        (
            "123",
            None,
            "http://www.w3.org/2001/XMLSchema#integer",
            "Literal('123', langtag=None, datatype='http://www.w3.org/2001/XMLSchema#integer')",
        ),
        (
            "3.14",
            None,
            "http://www.w3.org/2001/XMLSchema#float",
            "Literal('3.14', langtag=None, datatype='http://www.w3.org/2001/XMLSchema#float')",
        ),
        (
            "true",
            None,
            "http://www.w3.org/2001/XMLSchema#boolean",
            "Literal('true', langtag=None, datatype='http://www.w3.org/2001/XMLSchema#boolean')",
        ),
        (
            'He said "hello"',
            None,
            None,
            "Literal('He said \"hello\"', langtag=None, datatype=None)",
        ),
        (
            "line1\nline2",
            None,
            None,
            "Literal('line1\\nline2', langtag=None, datatype=None)",
        ),
        ("tab\there", None, None, "Literal('tab\\there', langtag=None, datatype=None)"),
        ("résumé", "fr", None, "Literal('résumé', langtag='fr', datatype=None)"),
        ("日本語", "ja", None, "Literal('日本語', langtag='ja', datatype=None)"),
        ("🚀", None, None, "Literal('🚀', langtag=None, datatype=None)"),
        (" ", None, None, "Literal(' ', langtag=None, datatype=None)"),
        ("", "en", None, "Literal('', langtag='en', datatype=None)"),
        (
            "",
            None,
            "http://www.w3.org/2001/XMLSchema#string",
            "Literal('', langtag=None, datatype='http://www.w3.org/2001/XMLSchema#string')",
        ),
    ],
)
def test_literal_repr(
    lex: str, langtag: str, datatype: str, expected_repr: str
) -> None:
    literal = Literal(lex, langtag=langtag, datatype=datatype)
    assert repr(literal) == expected_repr


@pytest.mark.parametrize(
    (
        "iri",
        "expected_str",
    ),
    [
        ("http://example.com", "<http://example.com>"),
        ("https://example.com", "<https://example.com>"),
        ("http://example.com/resource", "<http://example.com/resource>"),
        (
            "https://example.com/path/to/resource",
            "<https://example.com/path/to/resource>",
        ),
        ("http://example.com#fragment", "<http://example.com#fragment>"),
        ("http://example.com?query=value", "<http://example.com?query=value>"),
        (
            "http://example.com/resource?query=value#fragment",
            "<http://example.com/resource?query=value#fragment>",
        ),
        ("ftp://example.com", "<ftp://example.com>"),
        ("mailto:user@example.com", "<mailto:user@example.com>"),
        ("file:///path/to/file", "<file:///path/to/file>"),
        ("urn:isbn:0451450523", "<urn:isbn:0451450523>"),
        ("http://example.com/résource", "<http://example.com/résource>"),
        (
            "http://example.com/resource with spaces",
            "<http://example.com/resource with spaces>",
        ),
        ("http://example.com/München", "<http://example.com/München>"),
        ("http://example.com/日本語", "<http://example.com/日本語>"),
        ("", "<>"),
        ("relative/path", "<relative/path>"),
        ("//example.com", "<//example.com>"),
        (":", "<:>"),
        (
            "http://example.com/very/long/path/to/resource/with/many/segments",
            "<http://example.com/very/long/path/to/resource/with/many/segments>",
        ),
        (
            "http://example.com/path-with-dashes",
            "<http://example.com/path-with-dashes>",
        ),
        (
            "http://example.com/path_with_underscores",
            "<http://example.com/path_with_underscores>",
        ),
        ("http://example.com/path.with.dots", "<http://example.com/path.with.dots>"),
        (
            "http://example.com/path~with~tildes",
            "<http://example.com/path~with~tildes>",
        ),
        (
            "http://example.com/path%20with%20encoded%20spaces",
            "<http://example.com/path%20with%20encoded%20spaces>",
        ),
        ("http://example.com/path%C3%A9", "<http://example.com/path%C3%A9>"),
    ],
)
def test_iri_str_representation(iri: str, expected_str: str) -> None:
    iri_obj = IRI(iri)
    assert str(iri_obj) == expected_str


@pytest.mark.parametrize(
    (
        "iri",
        "expected_repr",
    ),
    [
        ("http://example.com", "IRI(http://example.com)"),
        ("https://example.com", "IRI(https://example.com)"),
        ("http://example.com/resource", "IRI(http://example.com/resource)"),
        (
            "https://example.com/path/to/resource",
            "IRI(https://example.com/path/to/resource)",
        ),
        ("http://example.com#fragment", "IRI(http://example.com#fragment)"),
        ("http://example.com?query=value", "IRI(http://example.com?query=value)"),
        (
            "http://example.com/resource?query=value#fragment",
            "IRI(http://example.com/resource?query=value#fragment)",
        ),
        ("ftp://example.com", "IRI(ftp://example.com)"),
        ("mailto:user@example.com", "IRI(mailto:user@example.com)"),
        ("file:///path/to/file", "IRI(file:///path/to/file)"),
        ("urn:isbn:0451450523", "IRI(urn:isbn:0451450523)"),
        ("http://example.com/résource", "IRI(http://example.com/résource)"),
        (
            "http://example.com/resource with spaces",
            "IRI(http://example.com/resource with spaces)",
        ),
        ("http://example.com/München", "IRI(http://example.com/München)"),
        ("http://example.com/日本語", "IRI(http://example.com/日本語)"),
        ("", "IRI()"),
        ("relative/path", "IRI(relative/path)"),
        ("//example.com", "IRI(//example.com)"),
        (":", "IRI(:)"),
        (
            "http://example.com/very/long/path/to/resource/with/many/segments",
            "IRI(http://example.com/very/long/path/to/resource/with/many/segments)",
        ),
        (
            "http://example.com/path-with-dashes",
            "IRI(http://example.com/path-with-dashes)",
        ),
        (
            "http://example.com/path_with_underscores",
            "IRI(http://example.com/path_with_underscores)",
        ),
        ("http://example.com/path.with.dots", "IRI(http://example.com/path.with.dots)"),
        (
            "http://example.com/path~with~tildes",
            "IRI(http://example.com/path~with~tildes)",
        ),
        (
            "http://example.com/path%20with%20encoded%20spaces",
            "IRI(http://example.com/path%20with%20encoded%20spaces)",
        ),
        ("http://example.com/path%C3%A9", "IRI(http://example.com/path%C3%A9)"),
    ],
)
def test_iri_repr(iri: str, expected_repr: str) -> None:
    iri_obj = IRI(iri)
    assert repr(iri_obj) == expected_repr


def test_parse_serialize() -> None:
    sink = GenericStatementSink()
    input_file_path = Path(
        "./tests/integration_tests/test_rdflib/temp/flat_output.jelly"
    )
    output_file_path = Path(
        "./tests/integration_tests/test_rdflib/temp/temp_output.jelly"
    )
    with input_file_path.open("rb") as file:
        sink.parse(file)
    with output_file_path.open("wb") as file:
        sink.serialize(file)
    new_sink = GenericStatementSink()
    with output_file_path.open("rb") as file:
        new_sink.parse(file)
    assert len(new_sink) > 0
    assert len(sink) > 0
    assert len(new_sink) == len(sink)
    for s_in, s_out in zip(sink.store, new_sink.store):
        assert repr(s_in) == repr(s_out)
