from __future__ import annotations

import io
from collections.abc import Callable, Iterable, Sequence
from typing import Any

import pytest
from rdflib import RDF, XSD, BNode, Dataset, Graph, Literal, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.plugins.stores.memory import Memory, SimpleMemory

from pyjelly.integrations.rdflib import walk


def ex(name: str) -> URIRef:
    return URIRef(f"http://example.org/{name}")


def add_statements(graph: Graph) -> None:
    for i in range(3):
        s = ex(f"s{i}")
        graph.add((s, RDF.type, ex("T")))
        graph.add((s, ex("p"), Literal("x", lang="en")))
        graph.add((s, ex("p"), Literal("1", datatype=XSD.integer)))
        graph.add((s, ex("q"), BNode("b")))
        graph.add((s, ex("q"), ex("o")))


def make_dataset(store: Memory | None = None) -> Dataset:
    dataset = Dataset(store=store if store is not None else Memory())
    # The same subjects in the default graph, a named graph and a blank node graph
    for name in (DATASET_DEFAULT_GRAPH_ID, ex("g"), BNode("g")):
        add_statements(dataset.graph(name))
    return dataset


def make_graph(store: Memory | None = None) -> Graph:
    graph = Graph(store=store if store is not None else Memory())
    add_statements(graph)
    return graph


def make_shared_graph() -> Graph:
    """Return a graph that shares its store with other graphs."""
    return make_dataset().graph(ex("g"))


def walk_triples(graph: Graph) -> list[tuple[Any, ...]]:
    tree = walk.spo_tree(graph)
    assert tree is not None
    return [(s, p, o) for s, ps in tree.items() for p, os in ps.items() for o in os]


def walk_quads(dataset: Dataset) -> list[tuple[Any, ...]]:
    trees = walk.gspo_trees(dataset)
    assert trees is not None
    return [
        (s, p, o, g)
        for g, tree in trees
        for s, ps in tree.items()
        for p, os in ps.items()
        for o in os
    ]


def public_path_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(walk, "_memory_indexes", lambda _store: None)


def assert_grouped(
    statements: Sequence[tuple[Any, ...]], key: Callable[..., Any]
) -> None:
    """Assert that statements with the same key come one after another."""
    seen = set()
    previous = object()
    for statement in statements:
        current = key(statement)
        if current != previous:
            assert current not in seen, f"{current} is not in one group"
            seen.add(current)
            previous = current


def test_memory_fast_path_is_used() -> None:
    # If this fails, rdflib changed the private fields of its in-memory store.
    # Writing still works through the public API, only slower: update
    # walk._memory_indexes for the new rdflib.
    assert walk._memory_indexes(Graph().store) is not None
    assert walk._memory_indexes(Dataset().store) is not None


def test_memory_fast_path_not_used_for_other_stores() -> None:
    assert walk._memory_indexes(Graph(store=SimpleMemory()).store) is None


def test_memory_fast_path_falls_back_when_fields_are_missing() -> None:
    store = Memory()
    del store._Memory__spo  # type: ignore[attr-defined]
    assert walk._memory_indexes(store) is None


@pytest.mark.parametrize(
    "make", [make_graph, make_shared_graph, Graph], ids=["alone", "shared", "empty"]
)
def test_fast_and_public_paths_give_same_triples(
    make: Callable[[], Graph], monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = make()
    fast = walk_triples(graph)
    public_path_only(monkeypatch)
    public = walk_triples(graph)

    for triples in (fast, public):
        assert len(triples) == len(graph)
        assert set(triples) == set(graph)
        assert_grouped(triples, lambda t: t[0])
        assert_grouped(triples, lambda t: t[:2])


@pytest.mark.parametrize("make", [make_dataset, Dataset], ids=["dataset", "empty"])
def test_fast_and_public_paths_give_same_quads(
    make: Callable[[], Dataset], monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset = make()
    expected = set(dataset.quads((None, None, None, None)))
    fast = walk_quads(dataset)
    public_path_only(monkeypatch)
    public = walk_quads(dataset)

    for quads in (fast, public):
        assert len(quads) == len(expected)
        assert set(quads) == expected
        assert_grouped(quads, lambda q: q[3])
        assert_grouped(quads, lambda q: (q[3], q[0]))
        assert_grouped(quads, lambda q: (q[3], q[0], q[1]))


def round_trip(graph: Graph, statements: Callable[[Graph], Iterable[Any]]) -> set[Any]:
    out = io.BytesIO()
    graph.serialize(destination=out, format="jelly")
    read = Dataset() if isinstance(graph, Dataset) else Graph()
    read.parse(data=out.getvalue(), format="jelly")
    return set(statements(read))


class OtherStore(Memory):
    """A store that the walker treats like any store that isn't in memory."""


@pytest.mark.parametrize("path", ["fast", "public", "other store"])
def test_serialize_round_trip(path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    if path == "public":
        public_path_only(monkeypatch)
    store = OtherStore if path == "other store" else Memory

    def quads(dataset: Graph) -> Iterable[Any]:
        assert isinstance(dataset, Dataset)
        return dataset.quads((None, None, None, None))

    dataset = make_dataset(store())
    assert round_trip(dataset, quads) == set(quads(dataset))
    graph = make_graph(store())
    assert round_trip(graph, set) == set(graph)
    shared = dataset.graph(ex("g"))
    assert round_trip(shared, set) == set(shared)


def test_other_stores_are_not_grouped_in_memory() -> None:
    # Such a store can be far too big to copy into memory: its statements are
    # written in its own order
    assert walk.spo_tree(make_graph(OtherStore())) is None
    assert walk.gspo_trees(make_dataset(OtherStore())) is None
