from __future__ import annotations

import re
import warnings
from collections import deque
from collections.abc import Generator
from io import TextIOWrapper
from pathlib import Path
from typing import NamedTuple, Union

DEFAULT_GRAPH_IDENTIFIER = ""


class BlankNode:
    """Class for blank nodes, storing BN's identifier as a string."""

    def __init__(self, identifier: str) -> None:
        self._identifier: str = identifier

    def __str__(self) -> str:
        return f"_:{self._identifier}"

    def __repr__(self) -> str:
        return f"BlankNode(identifier={self._identifier})"


class IRI:
    """Class for IRIs, storing IRI as a string."""

    def __init__(self, iri: str) -> None:
        self._iri: str = iri

    def __str__(self) -> str:
        return f"<{self._iri}>"

    def __repr__(self) -> str:
        return f"IRI({self._iri})"


class Literal:
    """
    Class for literals.

    Notes:
        Consists of: lexical form, and optional language tag and datatype.
        All parts of literal are stored as strings.

    """

    def __init__(
        self, lex: str, langtag: str | None = None, datatype: str | None = None
    ) -> None:
        self._lex: str = lex
        self._langtag: str | None = langtag
        self._datatype: str | None = datatype

    def __str__(self) -> str:
        suffix = ""
        if self._langtag:
            suffix = f"@{self._langtag}"
        elif self._datatype:
            suffix = f"^^<{self._datatype}>"
        return f'"{self._lex}"{suffix}'

    def __repr__(self) -> str:
        return (
            f"Literal({self._lex!r}, langtag={self._langtag!r}, "
            f"datatype={self._datatype!r})"
        )


Node = Union[BlankNode, IRI, Literal, "Triple", str]


TRIPLE_ARITY = 3


class Triple(NamedTuple):
    """Class for RDF triples."""

    s: Node
    p: Node
    o: Node


class Quad(NamedTuple):
    """Class for RDF quads."""

    s: Node
    p: Node
    o: Node
    g: Node


class Prefix(NamedTuple):
    """Class for generic namespace declaration."""

    prefix: str
    iri: IRI


class GenericStatementSink:
    _store: deque[Triple | Quad]

    def __init__(self, identifier: Node = DEFAULT_GRAPH_IDENTIFIER) -> None:
        """
        Initialize statements storage, namespaces dictionary, and parser.

        Notes:
            _store preserves the order of statements.

        Args:
            identifier (str, optional): Identifier for a sink.
                Defaults to DEFAULT_GRAPH_IDENTIFIER.

        """
        self._store: deque[Triple | Quad] = deque()
        self._namespaces: dict[str, IRI] = {}
        self._parser: GenericSinkParser = GenericSinkParser()
        self._identifier = identifier

    def add(self, statement: Triple | Quad) -> None:
        self._store.append(statement)

    def bind(self, prefix: str, namespace: IRI) -> None:
        self._namespaces.update({prefix: namespace})

    def __iter__(self) -> Generator[Triple | Quad]:
        yield from self._store

    def __len__(self) -> int:
        return len(self._store)

    @property
    def namespaces(self) -> Generator[tuple[str, IRI]]:
        yield from self._namespaces.items()

    @property
    def identifier(self) -> Node:
        return self._identifier

    @property
    def store(self) -> Generator[Triple | Quad]:
        yield from self._store

    @property
    def is_triples_sink(self) -> bool:
        """
        Check if the sink contains triples or quads.

        Returns:
            bool: true, if length of statement is 3.

        """
        return len(self._store[0]) == TRIPLE_ARITY

    def _serialize_node(self, node: Node) -> str:
        """
        Serialize node to its string representation.

        Args:
            node (Node): Node to convert - RDF term, str, or Triple.

        Returns:
            str: string representation of Node.

        """
        if isinstance(node, Triple):
            quoted_triple = [self._serialize_node(t) for t in node]
            return "<< " + " ".join(quoted_triple) + " >>"
        return str(node)

    def serialize(self, output_filename: Path, encoding: str = "utf-8") -> None:
        """
        Serialize sink's store content to a simple N-triples/N-quads format.

        Args:
            output_filename (Path): path to the output file.
            encoding (str): encoding of output. Defaults to utf-8.

        """
        with output_filename.open("w", encoding=encoding) as output_file:
            for statement in self._store:
                output_file.write(
                    " ".join(self._serialize_node(t) for t in statement) + " .\n"
                )

    def parse(self, input_filename: Path) -> None:
        """
        Parse content from input file.

        Args:
            input_filename (Path): path to input file.

        """
        warnings.warn(
            (
                "This is a minimal parser for the NT/NQ format, "
                "not intended for use outside of conformance tests. "
                "Proceed with caution."
            ),
            category=UserWarning,
            stacklevel=2,
        )

        statement_structure = (
            Quad if str(input_filename).split(".")[-1] in ("nq") else Triple
        )
        with input_filename.open("r") as input_file:
            for event in self._parser.parse(input_file, statement_structure):
                if isinstance(event, Prefix):
                    self.bind(*event)
                else:
                    self.add(event)


class GenericSinkParser:
    _uri_re = re.compile(r"<([^>\s]+)>")  # returns URI
    _bn_re = re.compile(r"_:(\S+)")  # returns blank node identificator
    _literal_re = re.compile(
        r""""([^"]*)"(?:@(\S+)|\^\^<(\S+)>)?"""
    )  # returns lex part of the literal and optional langtag and datatype
    _quoted_triple_re = re.compile(
        r"<<.*?>>"
    )  # matches the quoted triple quotation marks syntax
    _token_quoted_triple_re = re.compile(
        r"<<\s*(.*?)\s*>>"
    )  # returns the quoted triple
    _prefix_re = re.compile(
        r"@prefix\s+(\w+):\s*<([^>]+)>\s*\."
    )  # returns prefix and namespace IRI
    _split_tokens = re.compile(  # matches str to quoted triple/literal, IRI, or BN
        r"""
        <<.+?>>         |
        "[^"]+"(?:@\S+|\^\^\S+)?  |
        <[^>]+>         |
        _:\S+           |
        """,
        re.VERBOSE,
    )

    def process_term(
        self, term: str, slot: str
    ) -> IRI | BlankNode | Literal | Triple | str:
        """
        Process one term.

        Notes:
            terms are not validated to match RDF spec.

        Args:
            term (str): term to process
            slot (str): data structure to associate with terms

        Raises:
            TypeError: if literal decoded has multiple parts except for lex, i.e.,
                possibly both langtag and datatype are specified.
            TypeError: if fail to parse quoted triple into valid s/p/o.
            TypeError: if term did not match any pattern

        Returns:
            IRI | BlankNode | Literal | Triple | str: processed term

        """
        match_bn = self._bn_re.match(term)
        if match_bn:
            return BlankNode(match_bn.groups()[0])
        match_iri = self._uri_re.match(term)
        if match_iri:
            return IRI(match_iri.groups()[0])
        match_literal = self._literal_re.match(term)
        if match_literal:
            lex, langtag, datatype = match_literal.groups()
            if not lex or (langtag is not None and datatype is not None):
                msg = "invalid literal encountered"
                raise TypeError(msg)
            return Literal(lex, langtag, datatype)

        match_quoted_triple = self._quoted_triple_re.match(term)
        if match_quoted_triple:
            quoted_triple = self._token_quoted_triple_re.search(term)
            if quoted_triple:
                triple_tokens = self.generate_statement_tokens(
                    quoted_triple.groups()[0]
                )
                return Triple(
                    *(
                        self.process_term(group, slot)
                        for slot, group in zip(Triple._fields, triple_tokens)
                    )
                )
            msg = "invalid quoted triple encountered"
            raise TypeError(msg)

        if term == "" and slot == "g":
            return DEFAULT_GRAPH_IDENTIFIER

        msg = "failed to parse input file"
        raise TypeError(msg)

    def generate_statement_tokens(self, statement: str) -> list[str]:
        """
        Split statement into separate tokens.

        Notes:
            Tokens are not validated to follow RDF spec.

        Args:
            statement (str): Triple/Quad to split.

        Returns:
            list[str]: tokens to further process, matching simple
                IRI, Literal, blank node, quoted triple formats.

        """
        return [
            m.group(0)
            for m in self._split_tokens.finditer(statement)
            if m.group(0).strip()
        ]

    def parse_statement(
        self, statement: str, statement_structure: type[Triple | Quad]
    ) -> Triple | Quad:
        """
        Create Triple/Quad from statement string.

        Args:
            statement (str): full statement string (triple/quad).
            statement_structure (type[Triple  |  Quad]): a data structure for statement.

        Returns:
            Triple | Quad: resulting triple/quad

        """
        terms = self.generate_statement_tokens(statement)
        if len(terms) == TRIPLE_ARITY and statement_structure == Quad:
            terms.append("")  # append default graph identificator
        generic_terms = (
            self.process_term(term.strip(), slot)
            for slot, term in zip(statement_structure._fields, terms)
        )
        return statement_structure(*generic_terms)

    def parse_prefix(self, namespace: str) -> Prefix:
        """
        Create Prefix from namespace declaration string.

        Args:
            namespace (str): plain namespace declaration string.

        Raises:
            TypeError: raised if fail to match prefix and namespace IRI.

        Returns:
            Prefix: resulting Prefix

        """
        matched_namespace_declaration = self._prefix_re.match(namespace)
        if matched_namespace_declaration:
            matched_parts = matched_namespace_declaration.groups()
            return Prefix(matched_parts[0], IRI(matched_parts[1]))
        msg = "failed to parse namespace declaration"
        raise TypeError(msg)

    def parse_line(
        self, line: str, statement_structure: type[Triple | Quad]
    ) -> Triple | Quad | Prefix:
        """
        Parse one line from input into a respective statement or prefix.

        Args:
            line (str): plain line from source file.
            statement_structure (type[Triple  |  Quad]): a data structure for statement.

        Returns:
            Triple | Quad | Prefix: resulting parsed statement or namespace declaration.

        """
        if line.startswith("@prefix"):
            return self.parse_prefix(line)
        return self.parse_statement(line, statement_structure)

    def parse(
        self, input_file: TextIOWrapper, statement_structure: type[Triple | Quad]
    ) -> Generator[Triple | Quad | Prefix]:
        """
        Parse input lines into statements and prefixes.

        Note:
            comments and blnk lines in input file are ignored.

        Yields:
            Generator[Triple | Quad | Prefix]: yields statements and prefixes

        """
        for line in input_file:
            line_trimmed = line.strip()
            comment_index = line_trimmed.find("#")
            if comment_index == 0 or len(line_trimmed) == 0:
                continue
            yield self.parse_line(line_trimmed, statement_structure)
