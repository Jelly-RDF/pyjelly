from typing import Callable, Union, cast

import pytest
from rdflib import Graph
from rdflib.graph import Dataset

from pyjelly import jelly
from pyjelly.integrations.rdflib.serialize import guess_options, guess_stream
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import SerializerOptions

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
    ("make_store", "logical"),
    [
        (Graph, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (Dataset, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["graph-override", "dataset-override"],
)
def test_override_rdflib(
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
    stream = guess_stream(user_opts, store)
    # stream_options() returns None; inspect fields directly
    assert stream.stream_types.logical_type == logical
    assert stream.options.params.rdf_star is True
    assert stream.options.params.generalized_statements is True
