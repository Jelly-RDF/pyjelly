"""
SPARQL query result ser/des implementations for benchmarking.

Each method writes the rows of a result set to a stream, and reads them back from
bytes in memory.
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod
from collections import Counter
from typing import IO, Any
from typing_extensions import override

from rdflib import BNode, Literal, URIRef
from rdflib.query import Result

from benchmarks.sparql_data import SparqlBenchData
from pyjelly.integrations.generic import generic_sink
from pyjelly.integrations.generic import sparql as generic_sparql
from pyjelly.options import STRING_DATATYPE_IRI


class SparqlMethod(ABC):
    name: str
    #: Whether the library can write the format. If not, `write` is only a helper
    #: that produces the input for reading, and writing isn't measured.
    can_write: bool = True

    def supports(self, data: SparqlBenchData) -> bool:  # noqa: ARG002
        return True

    @abstractmethod
    def rows(self, data: SparqlBenchData) -> list[tuple[Any, ...]]:
        """Get the rows as this method's terms, None if unbound."""

    def prepare(self, data: SparqlBenchData) -> None:
        """Build the writer's input in advance, outside the measurement."""
        self.rows(data)

    def write_count(self, data: SparqlBenchData) -> int:
        """Count the cells that `write` writes."""
        return data.cells

    @abstractmethod
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        """Write the result set to `out`."""

    @abstractmethod
    def read(self, data: bytes, *, quads: bool = False) -> int:
        """
        Read the rows without keeping them, and return the number of cells read.

        `quads` is unused. It's there to match the RDF methods.
        """

    @abstractmethod
    def read_all(self, data: bytes) -> list[tuple[Any, ...]]:
        """Read and return the rows, for the round-trip check."""

    def write_bytes(self, data: SparqlBenchData) -> bytes:
        out = io.BytesIO()
        self.write(data, out)
        return out.getvalue()


def _cells(rows: int, variables: int) -> int:
    return rows * max(1, variables)


class RdflibResults(SparqlMethod):
    """An rdflib result format, written with Result.serialize, read with parse."""

    def __init__(self, name: str, rdflib_format: str) -> None:
        self.name = name
        self.format = rdflib_format

    @override
    def supports(self, data: SparqlBenchData) -> bool:
        return not data.rdf_12

    @override
    def rows(self, data: SparqlBenchData) -> list[tuple[Any, ...]]:
        result = data.rdflib
        variables = result.vars or []
        return [tuple(b.get(v) for v in variables) for b in result.bindings]

    @override
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        data.rdflib.serialize(destination=out, format=self.format)

    def _parse(self, data: bytes) -> Result:
        return Result.parse(io.BytesIO(data), format=self.format)

    @override
    def read(self, data: bytes, *, quads: bool = False) -> int:
        result = self._parse(data)
        return _cells(len(result.bindings), len(result.vars or []))

    @override
    def read_all(self, data: bytes) -> list[tuple[Any, ...]]:
        result = self._parse(data)
        variables = result.vars or []
        return [tuple(b.get(v) for v in variables) for b in result.bindings]


def _tsv_term(term: Any) -> str:
    """Format a term for SPARQL TSV results, in N-Triples syntax."""
    if term is None:
        return ""
    if isinstance(term, URIRef):
        return f"<{term}>"
    if isinstance(term, BNode):
        return f"_:{term}"
    lex = (
        str(term)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    if term.language:
        return f'"{lex}"@{term.language}'
    if term.datatype is not None and str(term.datatype) != STRING_DATATYPE_IRI:
        return f'"{lex}"^^<{term.datatype}>'
    return f'"{lex}"'


class RdflibTsv(RdflibResults):
    """
    SPARQL TSV results, read by rdflib.

    rdflib has no TSV writer, so `write` is a helper written for these benchmarks.
    """

    can_write = False

    def __init__(self) -> None:
        super().__init__("rdflib-tsv", "tsv")

    @override
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        lines = ["\t".join(f"?{name}" for name in data.variables)]
        lines += ["\t".join(map(_tsv_term, row)) for row in self.rows(data)]
        out.write(("\n".join(lines) + "\n").encode())


class GenericJelly(SparqlMethod):
    """Jelly-SPARQL with pyjelly's generic API and its own terms."""

    name = "generic-jelly"

    @override
    def rows(self, data: SparqlBenchData) -> list[tuple[Any, ...]]:
        return data.generic

    @override
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        generic_sparql.serialize_jelly_sparql(data.variables, data.generic, out)

    @override
    def read(self, data: bytes, *, quads: bool = False) -> int:
        results = generic_sparql.parse_jelly_sparql(io.BytesIO(data))
        rows = 0
        for _ in results:
            rows += 1
        return _cells(rows, len(results.variables))

    @override
    def read_all(self, data: bytes) -> list[tuple[Any, ...]]:
        return list(generic_sparql.parse_jelly_sparql(io.BytesIO(data)))


SPARQL_METHODS: dict[str, SparqlMethod] = {
    method.name: method
    for method in (
        RdflibResults("rdflib-srj", "json"),
        RdflibResults("rdflib-srx", "xml"),
        RdflibResults("rdflib-csv", "csv"),
        RdflibTsv(),
        # Jelly through rdflib's plugin API: Result.serialize and Result.parse
        RdflibResults("rdflib-jelly", "jelly"),
        GenericJelly(),
    )
}


def _normalize(term: Any) -> Any:
    """Normalize a term for comparison. Blank node labels are ignored."""
    if term is None:
        return None
    if isinstance(term, (BNode, generic_sink.BlankNode)):
        return "_:"
    if isinstance(term, (URIRef, Literal)):
        return term.n3()
    return str(term)


def sparql_round_trip(
    method: SparqlMethod, data: SparqlBenchData
) -> tuple[bytes, list[str]]:
    """
    Write the result set with the method, read it back, and compare the rows.

    Returns the written bytes and a list of problems (empty if none). Raises the
    reader's exception if the method can't read its own output.
    """
    written = method.write_bytes(data)
    expected = [tuple(map(_normalize, row)) for row in method.rows(data)]
    actual = [tuple(map(_normalize, row)) for row in method.read_all(written)]
    problems = []
    if method.read(written) != method.write_count(data):
        problems.append("read() counts a different number of cells than written")
    missing = Counter(expected) - Counter(actual)
    extra = Counter(actual) - Counter(expected)
    if missing or extra:
        problems.append(
            f"{sum(missing.values())} rows missing, {sum(extra.values())} unexpected; "
            f"e.g. missing {next(iter(missing), None)}, "
            f"unexpected {next(iter(extra), None)}"
        )
    elif expected != actual:
        problems.append("the rows are in another order")
    return written, problems
