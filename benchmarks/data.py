"""
RiverBench datasets, downloaded once and loaded as lists of statements.

The same datasets and the same number of statements as in the Jelly-JVM benchmarks.
"""

from __future__ import annotations

import gzip
import re
import tarfile
import urllib.request
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from functools import cached_property
from itertools import islice
from pathlib import Path
from typing import Any

from rdflib import Dataset, Graph
from rdflib.term import Node

from pyjelly.integrations.generic import parse as generic_parse
from pyjelly.integrations.rdflib import parse as rdflib_parse

RIVERBENCH_VERSION = "dev"

# All RiverBench datasets except politiquices (too short) and yago-annotated-facts
# (RDF-star, which most formats can't write). The same list as in Jelly-JVM.
DATASETS = (
    "assist-iot-weather",
    "assist-iot-weather-graphs",
    "citypulse-traffic",
    "citypulse-traffic-graphs",
    "dbpedia-live",
    "digital-agenda-indicators",
    "linked-spending",
    "lod-katrina",
    "muziekweb",
    "nanopubs",
    "officegraph",
    "openaire-lod",
    "osm2rdf-denmark",
)

DEFAULT_ROWS = 100_000

DATA_DIR = Path(__file__).parent / "data" / "riverbench"

# A Turtle/TriG prefix declaration, in either the "@prefix" or the "PREFIX" style
_PREFIX_LINE = re.compile(
    r"\s*@?prefix\s+([^\s:]*):\s*<([^>]*)>\s*\.?\s*", re.IGNORECASE
)


def _file_url(dataset: str, file: str) -> str:
    return f"https://w3id.org/riverbench/datasets/{dataset}/{RIVERBENCH_VERSION}/files/{file}"


def fetch(datasets: Iterable[str], data_dir: Path = DATA_DIR) -> None:
    """
    Download the datasets that aren't downloaded yet.

    For each dataset, two files are saved: the 100K Jelly distribution, and the
    prefixes declared at the start of the dataset's first stream element. Delete
    the files to download them again.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "prefixes").mkdir(exist_ok=True)
    for dataset in datasets:
        target = data_dir / f"{dataset}.jelly.gz"
        if target.exists():
            print(f"{dataset}: already downloaded")
        else:
            url = _file_url(dataset, "jelly_100K.jelly.gz")
            print(f"{dataset}: downloading {url}")
            # Download to a temporary file first, so that an interrupted download
            # doesn't look like a finished one
            partial = target.with_suffix(".part")
            with urllib.request.urlopen(url) as response, partial.open("wb") as out:  # noqa: S310
                while chunk := response.read(1 << 20):
                    out.write(chunk)
            partial.rename(target)
            print(f"{dataset}: {target.stat().st_size / 1e6:.1f} MB")

        prefixes_file = data_dir / "prefixes" / f"{dataset}.ttl"
        if not prefixes_file.exists():
            prefixes = _fetch_prefixes(dataset)
            prefixes_file.write_text("".join(f"{line}\n" for line in prefixes))
            print(f"{dataset}: {len(prefixes)} prefixes")


def _fetch_prefixes(dataset: str) -> list[str]:
    """Return the prefix lines of the first file in the stream distribution."""
    url = _file_url(dataset, "stream_10K.tar.gz")
    # "r|gz" reads the archive as a stream, so only its start is downloaded
    with (
        urllib.request.urlopen(url) as response,  # noqa: S310
        tarfile.open(fileobj=response, mode="r|gz") as archive,
    ):
        for member in archive:
            if not member.isfile():
                continue
            content = archive.extractfile(member)
            assert content is not None
            lines = content.read().decode().splitlines()
            return [line for line in lines if _PREFIX_LINE.fullmatch(line)]
    msg = f"no file in {url}"
    raise ValueError(msg)


@dataclass
class BenchData:
    """
    The first `rows` statements of a dataset.

    The statements are in the dataset's order, duplicates included. They are
    decoded from Jelly with pyjelly, once for each term model that is used: rdflib
    terms for the rdflib integration and rdflib's own formats, and pyjelly's own
    terms for the generic integration.

    For rdflib's formats that can only be written from a whole graph, the
    statements are also put into an rdflib Graph or Dataset.
    """

    name: str
    rows: int
    path: Path
    prefixes: list[tuple[str, str]]
    quads: bool = field(init=False)

    def __post_init__(self) -> None:
        first = next(iter(self._read(rdflib_parse.parse_jelly_flat, 1)))
        self.quads = len(first) == 4  # noqa: PLR2004

    @cached_property
    def rdflib(self) -> list[Any]:
        """The statements as rdflib Triple or Quad tuples."""
        return self._read(rdflib_parse.parse_jelly_flat, self.rows)

    @cached_property
    def rdflib_graph(self) -> Graph:
        """
        The statements in an rdflib Graph (triples) or Dataset (quads).

        The graph has the dataset's prefixes. It has no duplicates, so it can have
        fewer statements than the list, see `rdflib_graph_size`.
        """
        graph: Graph
        if self.quads:
            graph = dataset = Dataset()
            # One Graph object for each graph name, as a user would keep them
            contexts: dict[Node, Graph] = {}
            for s, p, o, g in self.rdflib:
                context = contexts.get(g)
                if context is None:
                    context = contexts[g] = dataset.graph(g)
                context.add((s, p, o))
        else:
            graph = Graph(bind_namespaces="core")
            for triple in self.rdflib:
                graph.add(triple)
        for prefix, namespace in self.prefixes:
            graph.bind(prefix, namespace)
        return graph

    @cached_property
    def rdflib_graph_size(self) -> int:
        """Return how many statements are in `rdflib_graph`, in all its graphs."""
        if isinstance(self.rdflib_graph, Dataset):
            return sum(1 for _ in self.rdflib_graph.quads())
        return len(self.rdflib_graph)

    @cached_property
    def generic(self) -> list[Any]:
        """The statements as Triple or Quad tuples of the generic integration."""
        return self._read(generic_parse.parse_jelly_flat, self.rows)

    def _read(self, parse_flat: Any, rows: int) -> list[Any]:
        # The distributions are grouped streams, but they are read as flat ones
        with gzip.open(self.path) as f:
            events: Iterator[Any] = parse_flat(f)
            # Prefix declarations are events too, but they aren't statements
            statements = (e for e in events if isinstance(e, tuple) and len(e) >= 3)  # noqa: PLR2004
            result = list(islice(statements, rows))
        if len(result) < rows:
            msg = f"dataset {self.name} has only {len(result)} statements"
            raise ValueError(msg)
        return result


def load(
    dataset: str, rows: int = DEFAULT_ROWS, data_dir: Path = DATA_DIR
) -> BenchData:
    path = data_dir / f"{dataset}.jelly.gz"
    prefixes_file = data_dir / "prefixes" / f"{dataset}.ttl"
    if not path.exists() or not prefixes_file.exists():
        msg = f"dataset {dataset} is not downloaded, run: python -m benchmarks fetch"
        raise FileNotFoundError(msg)
    prefixes = []
    for line in prefixes_file.read_text().splitlines():
        match = _PREFIX_LINE.fullmatch(line)
        assert match, f"not a prefix declaration in {prefixes_file}: {line}"
        prefixes.append((match[1], match[2]))
    return BenchData(dataset, rows, path, prefixes)
