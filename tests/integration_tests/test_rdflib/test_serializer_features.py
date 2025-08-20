import pytest
from rdflib import Graph
from rdflib.graph import Dataset

from pyjelly import jelly
from pyjelly.options import StreamParameters
from pyjelly.integrations.rdflib.serialize import guess_options, guess_stream
from pyjelly.integrations.rdflib.streams import SerializerOptions


@pytest.mark.parametrize(
    "make_store,expected_logical",
    [
        (Graph, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (Dataset, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["graph->triples", "dataset->quads"],
)
def test_defaults_rdflib(make_store, expected_logical):
    store = make_store()
    opts = guess_options(store)
    assert opts.logical_type == expected_logical
    assert opts.params.rdf_star is False
    assert opts.params.generalized_statements is False


@pytest.mark.parametrize(
    "make_store,logical",
    [
        (Graph, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (Dataset, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["graph-override", "dataset-override"],
)
def test_override_rdflib(make_store, logical):
    store = make_store()
    user_opts = SerializerOptions(
        logical_type=logical,
        params=StreamParameters(
            rdf_star=True,
            generalized_statements=True,
        ),
    )
    stream = guess_stream(user_opts, store)
    so = stream.stream_options()
    assert so.stream_types.logical_type == logical
    assert so.params.rdf_star is True
    assert so.params.generalized_statements is True
