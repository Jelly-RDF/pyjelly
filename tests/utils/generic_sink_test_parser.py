from __future__ import annotations

import re
import warnings
from pathlib import Path

from pyjelly.integrations.generic.generic_sink import (
    DEFAULT_GRAPH_IDENTIFIER,
    IRI,
    TRIPLE_ARITY,
    BlankNode,
    GenericStatementSink,
    Literal,
    Prefix,
    Quad,
    Triple,
)


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

    def __init__(self, sink: GenericStatementSink) -> None:
        self._sink = sink

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
            triple_tokens = self.split_statement(term.strip())
            return Triple(
                *(
                    self.process_term(group, slot)
                    for slot, group in zip(Triple._fields, triple_tokens)
                )
            )
        if term == "" and slot == "g":
            return DEFAULT_GRAPH_IDENTIFIER

        msg = "failed to parse input file"
        raise TypeError(msg)

    def find_closing_quotation(self, text: str, start: int) -> int:
        """
        Find the closing >> to a passed one.

        Notes:
            Examines what is inside the current quoted triple.
            Counts nesting levels and reports only the paired
            closing quote.

        Args:
            text (str): the current quoted triple parsed
            start (int): location of the opning quotation

        Returns:
            int: position of the closing quotation mark.

        """
        depth = 1
        pos = start + 2
        while pos < len(text) - 1 and depth > 0:
            if text[pos : pos + 2] == "<<":
                depth += 1
                pos += 2
            elif text[pos : pos + 2] == ">>":
                depth -= 1
                pos += 2
            else:
                pos += 1
        return pos

    def process_quoted(self, statement: str) -> list[str]:
        """
        Process quoted triple or its part.

        Notes:
            Allows having more quoted triples inside.
            Decides whether to:
                - tokenize the current triple, if no more nesting found or a part of
                    quoted triple is given
                - return as is -- if the statement is this only triple,
                    used during splitting statements into s/p/o/g
                - split quoted triple further -- during active term processing,
                    goes until all nesting levels are exhausted and the first
                    if is triggered

        Args:
            statement (str): quoted triple to process or a part of one

        Returns:
            list[str]: list of processed triple terms

        """
        start = statement.find("<<")
        if start == -1:
            split = self.generate_statement_tokens(statement.strip())
        else:
            if start == 0 and self.find_closing_quotation(statement, start) == len(
                statement
            ):
                return [statement]
            split = self.split_statement(statement.strip())
        return split

    def split_statement(self, text: str) -> list[str]:
        """
        Split the statement based on quotation marks position.

        Notes:
            If no quotation is found, uses tokenizer right away.
            If quotation is found, locates the first quoted triple and
            its surrounding terms and processes them separately, combining
            all in s/p/o list.

        Args:
            text (str): quoted triple to process

        Returns:
            list[str]: list of processed triple terms

        """
        start = text.find("<<")
        if start == -1:
            return self.generate_statement_tokens(text)
        pos = self.find_closing_quotation(text, start)

        if start == 0 and pos == len(text):
            triple = text[start + 2 : pos - 2].strip()
            split_triple = self.process_quoted(triple)
        else:
            split_triple = []
            if start > 0:
                text_before_quoted_triple = text[:start].strip()
                split_triple.extend(self.process_quoted(text_before_quoted_triple))
            split_triple.append(text[start:pos].strip())
            if pos < len(text):
                text_after_quoted_triple = text[pos:].strip()
                split_triple.extend(self.process_quoted(text_after_quoted_triple))
        return split_triple

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
        terms = self.split_statement(statement)
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

    def parse(self, input_filename: Path) -> None:
        """
        Parse input lines into statements and prefixes.

        Note:
            comments and blank lines in input file are ignored.

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
            for line in input_file:
                line_trimmed = line[: line.rfind(".")] + line[line.rfind(".") + 1 :]
                line_trimmed = line_trimmed.strip()
                comment_index = line_trimmed.find("#")
                if comment_index == 0 or len(line_trimmed) == 0:
                    continue
                event = self.parse_line(line_trimmed, statement_structure)
                if isinstance(event, Prefix):
                    self._sink.bind(*event)
                else:
                    self._sink.add(event)
