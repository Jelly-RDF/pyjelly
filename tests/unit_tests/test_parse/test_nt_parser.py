import pytest

from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
    Literal,
    Triple,
)
from tests.utils.generic_sink_test_parser import GenericSinkParser


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
def test_literal_parsing(
    lex: str, langtag: str, datatype: str, expected_str: str
) -> None:
    """Test literals parsing."""
    literal = str(Literal(lex, langtag, datatype))
    test_statement = f"_:bn <http://www.examples.org/predicate> {literal} ."
    gp = GenericSinkParser(GenericStatementSink())
    triple = gp.parse_statement(test_statement, Triple)
    assert str(triple[2]) == expected_str
