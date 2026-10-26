from __future__ import annotations

import re
from pathlib import Path

from pyjelly.integrations.generic.generic_sink import (
    IRI,
    TRIPLE_ARITY,
    BlankNode,
    DefaultGraph,
    GenericStatementSink,
    Literal,
    Node,
    Quad,
    Triple,
)

_ESCAPES = {
    "t": "\t",
    "b": "\b",
    "n": "\n",
    "r": "\r",
    "f": "\f",
    '"': '"',
    "'": "'",
    "\\": "\\",
}
_UNICODE_ESCAPE_LENGTHS = {"u": 4, "U": 8}
_BNODE_LABEL = re.compile(r"[^\s<>()\"]+")
_LANGTAG = re.compile(r"@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*)(?:--(ltr|rtl))?")
_PREFIX = re.compile(r"@prefix\s+([\w-]*):\s*<([^>]*)>\s*\.")


class _Reader:
    """Reads the terms of one statement."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def error(self, message: str) -> ValueError:
        return ValueError(f"{message} at {self.pos}: {self.text!r}")

    def skip_spaces(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos] in " \t":
            self.pos += 1

    def at(self, token: str) -> bool:
        self.skip_spaces()
        return self.text.startswith(token, self.pos)

    def expect(self, token: str) -> None:
        if not self.at(token):
            msg = f"expected {token!r}"
            raise self.error(msg)
        self.pos += len(token)

    def term(self) -> Node:
        if self.at("<<("):
            self.pos += 3
            triple = Triple(self.term(), self.term(), self.term())
            self.expect(")>>")
            return triple
        if self.at("<<"):
            # A quoted triple of RDF-star, used by the Jelly-RDF 1.1 tests
            self.pos += 2
            triple = Triple(self.term(), self.term(), self.term())
            self.expect(">>")
            return triple
        if self.at("<"):
            end = self.text.index(">", self.pos)
            iri = self.unescape(self.text[self.pos + 1 : end])
            self.pos = end + 1
            return IRI(iri)
        if self.at("_:"):
            match = _BNODE_LABEL.match(self.text, self.pos + 2)
            if not match:
                msg = "expected a blank node label"
                raise self.error(msg)
            label = match.group().rstrip(".")
            self.pos += 2 + len(label)
            return BlankNode(label)
        if self.at('"'):
            return self.literal()
        msg = "expected a term"
        raise self.error(msg)

    def literal(self) -> Literal:
        self.pos += 1
        chars = []
        while self.text[self.pos] != '"':
            char = self.text[self.pos]
            if char == "\\":
                char, self.pos = self.escape(self.text, self.pos)
            else:
                self.pos += 1
            chars.append(char)
        self.pos += 1
        lex = "".join(chars)
        langtag = _LANGTAG.match(self.text, self.pos)
        if langtag:
            self.pos = langtag.end()
            return Literal(lex, langtag.group(1), None, langtag.group(2))
        if self.text.startswith("^^", self.pos):
            self.pos += 2
            datatype = self.term()
            assert isinstance(datatype, IRI)
            return Literal(lex, None, datatype._iri)
        return Literal(lex)

    @staticmethod
    def escape(text: str, pos: int) -> tuple[str, int]:
        """Return the character of the escape sequence at pos, and the pos after."""
        kind = text[pos + 1]
        length = _UNICODE_ESCAPE_LENGTHS.get(kind)
        if length:
            return chr(int(text[pos + 2 : pos + 2 + length], 16)), pos + 2 + length
        return _ESCAPES[kind], pos + 2

    def unescape(self, text: str) -> str:
        chars = []
        pos = 0
        while pos < len(text):
            if text[pos] == "\\":
                char, pos = self.escape(text, pos)
            else:
                char = text[pos]
                pos += 1
            chars.append(char)
        return "".join(chars)


class GenericSinkParser:
    """
    Parser of N-Triples and N-Quads into generic statements, for tests only.

    It reads the syntax of RDF 1.2 (triple terms, base directions), the quoted
    triples of RDF-star, and generalized statements, as used by the Jelly
    conformance tests. It does not check that the input is valid.
    """

    def __init__(self, sink: GenericStatementSink) -> None:
        self._sink = sink

    def parse_statement(
        self, statement: str, statement_structure: type[Triple | Quad]
    ) -> Triple | Quad:
        """Parse a statement, with or without the dot at its end."""
        reader = _Reader(statement)
        terms = []
        while True:
            reader.skip_spaces()
            if reader.pos == len(statement) or reader.at("."):
                break
            terms.append(reader.term())
        if len(terms) == TRIPLE_ARITY:
            s, p, o = terms
            if statement_structure is Quad:
                return Quad(s, p, o, DefaultGraph)
            return Triple(s, p, o)
        s, p, o, g = terms
        return Quad(s, p, o, g)

    def parse(self, input_filename: Path) -> None:
        """Parse a file into the sink. `@prefix` lines declare namespaces."""
        statement_structure = Quad if input_filename.suffix == ".nq" else Triple
        for line in input_filename.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            prefix = _PREFIX.match(stripped)
            if prefix:
                self._sink.bind(prefix.group(1), IRI(prefix.group(2)))
            else:
                self._sink.add(self.parse_statement(stripped, statement_structure))
