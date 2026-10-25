"""
Walk rdflib graphs grouped by graph, subject and predicate (gspo order).

Jelly writes a term only when it differs from the same term in the previous
statement, and keeps recently used IRIs in lookup tables. Both work much better
when statements are sorted by their terms.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, NamedTuple

from rdflib import Dataset, Graph
from rdflib.plugins.stores.memory import Memory
from rdflib.store import Store

# subject -> predicate -> objects. The objects are a list, or the keys of a dict
# in the in-memory store's own index.
Tree = Mapping[Any, Mapping[Any, Iterable[Any]]]


def _group(triples: Iterable[tuple[Any, Any, Any]]) -> Tree:
    """Group triples by subject, then by predicate, in the order first seen."""
    tree: dict[Any, dict[Any, list[Any]]] = {}
    for s, p, o in triples:
        predicates = tree.get(s)
        if predicates is None:
            predicates = tree[s] = {}
        objects = predicates.get(p)
        if objects is None:
            predicates[p] = [o]
        else:
            objects.append(o)
    return tree


def _flatten(tree: Tree) -> Iterator[tuple[Any, Any, Any]]:
    for s, predicates in tree.items():
        for p, objects in predicates.items():
            for o in objects:
                yield s, p, o


class _MemoryIndexes(NamedTuple):
    """The private indexes of rdflib's in-memory store that the fast path uses."""

    #: Context key -> the set of its triples. None means all triples.
    context_triples: dict[str | None, set[tuple[Any, Any, Any]]]
    #: Context key -> the Graph (or name) it stands for
    context_objects: dict[str, Any]
    #: subject -> predicate -> object -> 1, for all triples in the store
    spo: Tree
    #: Makes the context key of a Graph
    context_key: Callable[[Graph], str]


def _memory_indexes(store: Store) -> _MemoryIndexes | None:
    """
    Return the in-memory store's private indexes, if this is that store.

    These fields are private to rdflib, but haven't changed since rdflib 6.0.
    If they are not there (e.g., renamed in a new rdflib version), return None,
    so that the slower public API is used instead. Tests check that this
    works for the installed rdflib version.
    """
    if type(store) is not Memory:
        return None
    try:
        return _MemoryIndexes(
            context_triples=getattr(store, "_Memory__contextTriples"),  # noqa: B009
            context_objects=getattr(store, "_Memory__context_obj_map"),  # noqa: B009
            spo=getattr(store, "_Memory__spo"),  # noqa: B009
            context_key=getattr(store, "_Memory__ctx_to_str"),  # noqa: B009
        )
    except AttributeError:
        return None


def spo_tree(graph: Graph) -> Tree | None:
    """
    Return the graph's triples as a subject -> predicate -> objects tree.

    Only for rdflib's in-memory store, whose triples are in memory anyway. For any
    other store, return None: such a store can be far too big to group in memory.
    Its triples are then written in the store's own order, which for stores with
    indexes (e.g., Oxigraph, BerkeleyDB) is usually already grouped by subject.
    """
    store = graph.store
    if type(store) is not Memory:
        return None
    indexes = _memory_indexes(store)
    if indexes is None:
        # The private fields are not there: group with the public API
        return _group(graph.triples((None, None, None)))
    contexts = indexes.context_triples
    context = indexes.context_key(graph)
    triples = contexts.get(context)
    if not triples:
        return {}
    if not any(key not in (None, context) and value for key, value in contexts.items()):
        # The graph is alone in its store, so the store's subject-predicate-object
        # index contains its triples, already grouped and in the order they
        # were added
        return indexes.spo
    return _group(triples)


def gspo_trees(dataset: Dataset) -> Iterator[tuple[Any, Tree]] | None:
    """
    Return the dataset's graphs, each as its name and its triples as a tree.

    Only for rdflib's in-memory store, see spo_tree(). Each graph is grouped only
    when it's its turn, so at most one graph's tree is in memory at a time.
    """
    store = dataset.store
    if type(store) is not Memory:
        return None
    indexes = _memory_indexes(store)
    if indexes is None:
        # The private fields are not there: group with the public API
        return _group_quads(dataset.quads((None, None, None, None)))
    return _memory_gspo_trees(indexes)


def _memory_gspo_trees(indexes: _MemoryIndexes) -> Iterator[tuple[Any, Tree]]:
    for context, triples in indexes.context_triples.items():
        # None is not a graph, but an index of all triples in the store
        if context is None or not triples:
            continue
        graph = indexes.context_objects[context]
        yield getattr(graph, "identifier", graph), _group(triples)


def _group_quads(
    quads: Iterable[tuple[Any, Any, Any, Any]],
) -> Iterator[tuple[Any, Tree]]:
    by_graph: dict[Any, list[tuple[Any, Any, Any]]] = {}
    for s, p, o, g in quads:
        triples_list = by_graph.get(g)
        if triples_list is None:
            by_graph[g] = [(s, p, o)]
        else:
            triples_list.append((s, p, o))
    for g, triples_of_graph in by_graph.items():
        yield g, _group(triples_of_graph)


def spo_triples(graph: Graph) -> Iterator[tuple[Any, Any, Any]]:
    """Return the graph's triples, grouped by subject and predicate if in memory."""
    tree = spo_tree(graph)
    if tree is None:
        return iter(graph.triples((None, None, None)))
    return _flatten(tree)


def gspo_quads(dataset: Dataset) -> Iterator[tuple[Any, Any, Any, Any]]:
    """Return the dataset's quads, grouped if in memory (see gspo_trees())."""
    trees = gspo_trees(dataset)
    if trees is None:
        yield from dataset.quads((None, None, None, None))
        return
    for g, tree in trees:
        for s, p, o in _flatten(tree):
            yield s, p, o, g
