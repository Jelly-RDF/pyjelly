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

from pyjelly import jelly
from pyjelly.serialize.streams import QuadStream, Stream

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


#: Whether to read the in-memory store's private indexes. Only for tests: the
#: slower public API gives the same statements.
memory_fast_path = True


def _memory_indexes(store: Store) -> _MemoryIndexes | None:
    """
    Return the in-memory store's private indexes, if this is that store.

    These fields are private to rdflib, but haven't changed since rdflib 6.0.
    If they are not there (e.g., renamed in a new rdflib version), return None,
    so that the slower public API is used instead. Tests check that this
    works for the installed rdflib version.
    """
    if type(store) is not Memory or not memory_fast_path:
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


def write_tree(stream: Stream, tree: Tree) -> Iterator[jelly.RdfStreamFrame]:
    """
    Write the statements of a subject -> predicate -> objects tree.

    Uses the stream's cursor API, so the stream doesn't compare terms. In a tree,
    each subject differs from the one before it, and so do the predicates of one
    subject and the objects of one predicate. Only the first subject of the tree
    and the first predicate of a subject can be equal to the one before it (e.g.,
    rdf:type as the first predicate of every subject), so only those are
    compared. The first object of a predicate is rarely equal to the object
    before it, and most predicates have only one object, so objects are not
    compared: an equal one is just written again.

    Yields:
        jelly.RdfStreamFrame: full frames.

    """
    encoder = stream.encoder
    terms = stream.repeated_terms
    first_subject = True
    for s, predicates in tree.items():
        if not first_subject or not encoder.same_term(terms[0], s):
            stream.set_subject(s)
        first_subject = False
        first_predicate = True
        for p, objects in predicates.items():
            if not first_predicate or not encoder.same_term(terms[1], p):
                stream.set_predicate(p)
            first_predicate = False
            for o in objects:
                stream.set_object(o)
                frame = stream.emit()
                if frame is not None:
                    yield frame


def write_trees(
    stream: QuadStream, trees: Iterable[tuple[Any, Tree]]
) -> Iterator[jelly.RdfStreamFrame]:
    """Write the statements of graphs, see gspo_trees() and write_tree()."""
    for g, tree in trees:
        if not stream.encoder.same_term(stream.repeated_terms[3], g):
            stream.set_graph(g)
        yield from write_tree(stream, tree)
