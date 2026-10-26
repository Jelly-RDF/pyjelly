"""
SPARQL query results for the Jelly-SPARQL benchmarks, as in Jelly-JVM.

A dataset is either a RiverBench dataset, read as the results of
`SELECT * WHERE { ?s ?p ?o }` (triples) or `SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }`
(quads, with ?g unbound in the default graph), or a synthetic result set made from
one of the presets of Jelly-JVM's generator (SparqlDataGen). Each preset brings
out one property of the format.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field, replace
from functools import cached_property
from typing import Any, ClassVar

from rdflib import BNode, Literal, URIRef, Variable
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.query import Result

from benchmarks import data as rdf_data
from pyjelly.integrations.generic import generic_sink as generic

# --- The generator, ported from Jelly-JVM's SparqlDataGen ---

IRI = "iri"
BNODE = "bnode"
PLAIN_LITERAL = "plain"
LANG_LITERAL = "lang"
DT_LITERAL = "typed"
DIR_LANG_LITERAL = "dirlang"
TRIPLE_TERM = "triple"

# How the unbound cells of a column are spread: each cell on its own, every
# second cell, or blocks of `block_size` cells
RANDOM = "random"
ALTERNATING = "alternating"
BLOCKS = "blocks"

_NAMESPACE_BASE = "https://example.org/ns"
_DATATYPE_BASE = "https://example.org/dt#"
_LANGS = ("en", "fr", "de", "es", "pl", "nl", "en-GB", "pt-BR")


@dataclass(frozen=True)
class ColumnSpec:
    """One variable of a generated result set. The defaults: dense IRIs."""

    kind: str = IRI
    #: How many distinct values the column draws from
    distinct_values: int = 1024
    #: How many IRI namespaces, datatypes, or language tags
    namespaces: int = 1
    #: Length of the local name of IRIs, or of the lexical form of literals
    value_length: int = 12
    #: How many of the cells are bound
    bound_fraction: float = 1.0
    sparsity: str = RANDOM
    block_size: int = 1
    #: Mean length of a run of one value in consecutive rows
    run_length: float = 1.0
    #: Whether runs have random lengths, or exactly `run_length`
    jitter_runs: bool = True
    #: Whether values are drawn in order (as with ORDER BY) or at random
    sorted: bool = False
    #: How many of the bound cells are of another term type
    mix_fraction: float = 0.0
    #: The first row with a value of the other term type
    mix_start_row: int = 0


@dataclass(frozen=True)
class ResultSetSpec:
    name: str
    columns: tuple[ColumnSpec, ...]
    rows: int
    seed: int = 0

    @property
    def variables(self) -> list[str]:
        return [f"v{i}" for i in range(len(self.columns))]


def _pad(base: str, length: int) -> str:
    return base if len(base) >= length else base + "x" * (length - len(base))


def _iri(column: ColumnSpec, j: int, name: str) -> generic.IRI:
    namespace = j % max(1, column.namespaces)
    return generic.IRI(
        f"{_NAMESPACE_BASE}{namespace}#{_pad(name, max(1, column.value_length))}"
    )


def _pool(column: ColumnSpec, kind: str) -> list[Any]:
    """Return the distinct values a column draws from, as generic terms."""
    n = max(1, column.distinct_values)
    length = max(1, column.value_length)
    if kind == IRI:
        return [_iri(column, j, f"term{j}") for j in range(n)]
    if kind == BNODE:
        return [generic.BlankNode(_pad(f"b{j}", min(length, 24))) for j in range(n)]
    if kind == TRIPLE_TERM:
        return [_triple_term(column, j) for j in range(n)]
    return _literal_pool(column, kind)


def _literal_pool(column: ColumnSpec, kind: str) -> list[Any]:
    n = max(1, column.distinct_values)
    ns = max(1, column.namespaces)
    length = max(1, column.value_length)
    if kind == PLAIN_LITERAL:
        return [generic.Literal(_pad(f"value {j} ", length)) for j in range(n)]
    if kind == LANG_LITERAL:
        langs = _LANGS[: min(ns, len(_LANGS))]
        return [
            generic.Literal(_pad(f"value {j} ", length), langs[j % len(langs)])
            for j in range(n)
        ]
    if kind == DT_LITERAL:
        return [
            generic.Literal(_pad(f"{j}", length), None, f"{_DATATYPE_BASE}d{j % ns}")
            for j in range(n)
        ]
    assert kind == DIR_LANG_LITERAL
    # Each "namespace" is one combination of a language tag and a direction
    return [
        generic.Literal(
            _pad(f"value {j} ", length),
            _LANGS[(j % ns) // 2 % len(_LANGS)],
            None,
            "ltr" if j % ns % 2 == 0 else "rtl",
        )
        for j in range(n)
    ]


def _triple_term(column: ColumnSpec, j: int) -> generic.Triple:
    """Return a triple term with fresh IRIs, some nested."""
    o: Any
    if j % 3 == 0:
        o = _iri(column, j, f"o{j}")
    elif j % 3 == 1:
        o = generic.Literal(f"value {j}", "en", None, "ltr" if j % 2 else "rtl")
    else:
        datatype = f"{_DATATYPE_BASE}d{j % max(1, column.namespaces)}"
        o = generic.Triple(
            generic.BlankNode(f"b{j}"),
            _iri(column, j, f"q{j}"),
            generic.Literal(f"{j}", None, datatype),
        )
    return generic.Triple(_iri(column, j, f"s{j}"), _iri(column, j, f"p{j % 16}"), o)


def _generate_column(column: ColumnSpec, rows: int, rnd: random.Random) -> list[Any]:
    pool = _pool(column, column.kind)
    other_kind = PLAIN_LITERAL if column.kind == IRI else IRI
    other_pool = _pool(column, other_kind) if column.mix_fraction > 0 else []
    cursor = 0

    def next_value(row: int) -> Any:
        nonlocal cursor
        use_other = (
            other_pool
            and row >= column.mix_start_row
            and rnd.random() < column.mix_fraction
        )
        values = other_pool if use_other else pool
        if column.sorted:
            value = values[cursor % len(values)]
            cursor += 1
            return value
        return values[rnd.randrange(len(values))]

    def run_length() -> int:
        if column.run_length <= 1:
            return 1
        if not column.jitter_runs:
            return max(1, round(column.run_length))
        # Geometric, with the mean run_length
        p = 1 / column.run_length
        n = 1
        while rnd.random() > p and n < 4096:  # noqa: PLR2004
            n += 1
        return n

    cells: list[Any] = []
    while len(cells) < rows:
        row = len(cells)
        if column.sparsity == ALTERNATING:
            cells.append(None if row % 2 else next_value(row))
        elif rnd.random() >= column.bound_fraction:
            block = max(1, column.block_size) if column.sparsity == BLOCKS else 1
            cells.extend([None] * min(block, rows - len(cells)))
        else:
            value = next_value(row)
            cells.extend([value] * min(run_length(), rows - len(cells)))
    return cells


def generate(spec: ResultSetSpec) -> list[tuple[Any, ...]]:
    """Return the rows of a result set, with generic terms and None if unbound."""
    if not spec.columns:
        return [()] * spec.rows
    columns = [
        _generate_column(column, spec.rows, random.Random(spec.seed * 31 + i))  # noqa: S311
        for i, column in enumerate(spec.columns)
    ]
    return list(zip(*columns, strict=True))


_N = 20_000

PRESETS: dict[str, ResultSetSpec] = {
    spec.name: spec
    for spec in (
        # IRIs: the lookups, and the prefix and name inference
        ResultSetSpec("iri-sorted", (ColumnSpec(distinct_values=_N, sorted=True),), _N),
        ResultSetSpec("iri-shuffled", (ColumnSpec(distinct_values=_N),), _N),
        ResultSetSpec("iri-small-pool", (ColumnSpec(distinct_values=2048),), _N),
        ResultSetSpec(
            "iri-many-ns", (ColumnSpec(distinct_values=2048, namespaces=32),), _N
        ),
        # Repeated values: the runs of the layouts
        ResultSetSpec(
            "runs-2",
            (ColumnSpec(distinct_values=2048, run_length=2, sorted=True),),
            _N,
        ),
        ResultSetSpec(
            "runs-8",
            (ColumnSpec(distinct_values=2048, run_length=8, sorted=True),),
            _N,
        ),
        ResultSetSpec(
            "alternating-values", (ColumnSpec(distinct_values=2, sorted=True),), _N
        ),
        # Unbound cells
        ResultSetSpec(
            "sparse-random-10",
            (ColumnSpec(distinct_values=2048, bound_fraction=0.1),),
            _N,
        ),
        ResultSetSpec(
            "sparse-random-50",
            (ColumnSpec(distinct_values=2048, bound_fraction=0.5),),
            _N,
        ),
        ResultSetSpec(
            "sparse-random-90",
            (ColumnSpec(distinct_values=2048, bound_fraction=0.9),),
            _N,
        ),
        ResultSetSpec(
            "sparse-blocks",
            (
                ColumnSpec(
                    distinct_values=2048,
                    bound_fraction=0.5,
                    sparsity=BLOCKS,
                    block_size=16,
                ),
            ),
            _N,
        ),
        ResultSetSpec(
            "sparse-alternating",
            (ColumnSpec(distinct_values=2048, sparsity=ALTERNATING),),
            _N,
        ),
        # Literals
        ResultSetSpec(
            "lit-plain", (ColumnSpec(PLAIN_LITERAL, distinct_values=2048),), _N
        ),
        ResultSetSpec(
            "lit-lang",
            (ColumnSpec(LANG_LITERAL, distinct_values=2048, namespaces=8),),
            _N,
        ),
        ResultSetSpec(
            "lit-lang-one",
            (ColumnSpec(LANG_LITERAL, distinct_values=2048, namespaces=1),),
            _N,
        ),
        ResultSetSpec(
            "lit-typed",
            (ColumnSpec(DT_LITERAL, distinct_values=2048, namespaces=8),),
            _N,
        ),
        ResultSetSpec(
            "lit-long",
            (ColumnSpec(PLAIN_LITERAL, distinct_values=512, value_length=4096),),
            2_000,
        ),
        ResultSetSpec("bnodes", (ColumnSpec(BNODE, distinct_values=2048),), _N),
        # Columns that mix term types
        ResultSetSpec(
            "poly-half", (ColumnSpec(distinct_values=2048, mix_fraction=0.5),), _N
        ),
        ResultSetSpec(
            "poly-late-switch",
            (
                ColumnSpec(
                    distinct_values=2048, mix_fraction=0.0005, mix_start_row=5_000
                ),
            ),
            _N,
        ),
        # RDF 1.2 terms
        ResultSetSpec(
            "lit-dirlang",
            (ColumnSpec(DIR_LANG_LITERAL, distinct_values=2048, namespaces=4),),
            _N,
        ),
        ResultSetSpec(
            "triple-terms",
            (
                ColumnSpec(TRIPLE_TERM, distinct_values=_N, namespaces=4),
                ColumnSpec(distinct_values=2048),
            ),
            _N,
        ),
        # The shape of the result set
        ResultSetSpec("wide-5", (ColumnSpec(distinct_values=2048),) * 5, _N),
        ResultSetSpec("wide-20", (ColumnSpec(distinct_values=2048),) * 20, _N // 2),
        ResultSetSpec("single-row", (ColumnSpec(distinct_values=8),) * 3, 1),
        ResultSetSpec("zero-vars", (), _N),
        # Entity, property, label, optional value, and a value of mixed types
        ResultSetSpec(
            "realistic-mixed",
            (
                ColumnSpec(
                    distinct_values=4096, run_length=3, sorted=True, value_length=8
                ),
                ColumnSpec(distinct_values=32, value_length=10),
                ColumnSpec(
                    LANG_LITERAL, distinct_values=4096, namespaces=4, value_length=24
                ),
                ColumnSpec(PLAIN_LITERAL, distinct_values=1024, bound_fraction=0.4),
                ColumnSpec(distinct_values=2048, mix_fraction=0.3),
            ),
            _N,
        ),
    )
}

#: The datasets of a run, unless chosen: all RiverBench datasets, and the presets
#: that look most like the results of real queries
DEFAULT_DATASETS = (
    *rdf_data.DATASETS,
    "realistic-mixed",
    "wide-20",
    "sparse-random-50",
    "lit-lang",
    "iri-shuffled",
)

DATASETS = (*rdf_data.DATASETS, *PRESETS)


def _resized(spec: ResultSetSpec, rows: int) -> ResultSetSpec:
    """Change the row count of a preset, but not its shape."""
    factor = rows / spec.rows
    return replace(
        spec,
        rows=rows,
        columns=tuple(
            replace(column, mix_start_row=int(column.mix_start_row * factor))
            for column in spec.columns
        ),
    )


# --- The results, in the terms of each library ---


def _rdflib_term(term: Any) -> Any:
    if term is None:
        return None
    if isinstance(term, generic.IRI):
        return URIRef(term._iri)
    if isinstance(term, generic.BlankNode):
        return BNode(term._id)
    if isinstance(term, generic.Literal):
        if term._direction:
            msg = "rdflib has no literals with a base direction"
            raise TypeError(msg)
        # As pyjelly reads them: the lexical form stays as it is
        return Literal(
            term._lex, lang=term._langtag, datatype=term._datatype, normalize=False
        )
    msg = "rdflib has no triple terms"
    raise TypeError(msg)


@dataclass
class SparqlBenchData:
    """
    A result set: its variables and rows, with generic terms and rdflib terms.

    `cells` counts what the throughput is measured in: rows times variables, and
    rows for a result set without variables.
    """

    #: As in the data of RDF, which the benchmark code also reads
    quads: ClassVar[bool] = False

    name: str
    variables: list[str]
    generic: list[tuple[Any, ...]] = field(repr=False)
    #: The rows with rdflib terms, if not made from the generic ones
    rdflib_rows: list[tuple[Any, ...]] | None = field(default=None, repr=False)

    @property
    def cells(self) -> int:
        return len(self.generic) * max(1, len(self.variables))

    @cached_property
    def rdf_12(self) -> bool:
        """Whether the results have terms of RDF 1.2, which rdflib has not."""
        return any(
            isinstance(term, generic.Triple)
            or (isinstance(term, generic.Literal) and term._direction)
            for row in self.generic
            for term in row
        )

    @cached_property
    def rdflib(self) -> Result:
        """The results as an rdflib Result, with the bindings in a list."""
        result = Result("SELECT")
        variables = [Variable(name) for name in self.variables]
        result.vars = variables
        rows = self.rdflib_rows
        if rows is None:
            rows = [tuple(map(_rdflib_term, row)) for row in self.generic]
        result.bindings = [
            {
                variable: term
                for variable, term in zip(variables, row, strict=True)
                if term is not None
            }
            for row in rows
        ]
        return result


def load(dataset: str, rows: int = rdf_data.DEFAULT_ROWS) -> SparqlBenchData:
    """Return the first `rows` rows of a dataset."""
    if dataset in PRESETS:
        spec = _resized(PRESETS[dataset], rows)
        return SparqlBenchData(dataset, spec.variables, generate(spec))
    statements = rdf_data.load(dataset, rows)
    if not statements.quads:
        return SparqlBenchData(
            dataset,
            ["s", "p", "o"],
            [tuple(statement) for statement in statements.generic],
            [tuple(statement) for statement in statements.rdflib],
        )
    return SparqlBenchData(
        dataset,
        ["s", "p", "o", "g"],
        [
            (s, p, o, None if g is generic.DefaultGraph else g)
            for s, p, o, g in statements.generic
        ],
        [
            (s, p, o, None if g == DATASET_DEFAULT_GRAPH_ID else g)
            for s, p, o, g in statements.rdflib
        ],
    )
