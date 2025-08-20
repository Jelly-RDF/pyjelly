from typing import Callable, Union, cast

import pytest
from rdflib import Graph
from rdflib.graph import Dataset

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError
from pyjelly.integrations.rdflib.serialize import guess_options, guess_stream
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import QuadStream, SerializerOptions, TripleStream

Store = Union[Graph, Dataset]


@pytest.mark.parametrize(
    ("make_store", "expected_logical"),
    [
        (Graph, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (Dataset, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["graph->triples", "dataset->quads"],
)
def test_defaults_rdflib(
    make_store: Callable[[], Store],
    expected_logical: int,
) -> None:
    store = make_store()
    opts = guess_options(store)
    assert opts.logical_type == expected_logical
    assert opts.params.rdf_star is False
    assert opts.params.generalized_statements is False


@pytest.mark.parametrize(
    ("make_store", "logical", "expected_physical"),
    [
        (
            Graph,
            jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            TripleStream,
        ),
        (
            Dataset,
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            QuadStream,
        ),
    ],
    ids=["graph+triples", "dataset+quads"],
)
def test_override_rdflib_compatible(
    make_store: Callable[[], Store],
    logical: int,
    expected_physical: type,
) -> None:
    store = make_store()
    user_opts = SerializerOptions(
        logical_type=cast(jelly.LogicalStreamType, logical),
        params=StreamParameters(
            rdf_star=True,
            generalized_statements=True,
        ),
    )
    stream = guess_stream(user_opts, store)

    assert isinstance(stream, expected_physical)
    assert stream.options.logical_type == logical
    assert stream.options.params.rdf_star is True
    assert stream.options.params.generalized_statements is True


@pytest.mark.parametrize(
    ("make_store", "logical"),
    [
        (Graph, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (Dataset, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
    ],
    ids=["graph+quads-error", "dataset+triples-error"],
)
def test_override_rdflib_incompatible(
    make_store: Callable[[], Store],
    logical: int,
) -> None:
    store = make_store()
    user_opts = SerializerOptions(
        logical_type=cast(jelly.LogicalStreamType, logical),
        params=StreamParameters(
            rdf_star=True,
            generalized_statements=True,
        ),
    )
    with pytest.raises(
        JellyAssertionError,
        match="is not compatible with",
    ):
        guess_stream(user_opts, store)
