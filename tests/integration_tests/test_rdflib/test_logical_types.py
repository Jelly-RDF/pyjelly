import io

import pytest
from rdflib import Dataset, Graph
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly import jelly
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.streams import GraphStream, QuadStream, SerializerOptions, Stream


def test_flat_triples() -> None:
    g_out = Graph()
    g_out.parse(source="tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")

    out = g_out.serialize(encoding="jelly", format="jelly")
    triples_out = set(g_out)
    assert len(triples_out) > 0

    g_in = Graph()
    g_in.parse(out, format="jelly")

    triples_in = set(g_in)

    assert len(triples_out) == len(triples_in)
    assert triples_in == triples_out

    options, _ = get_options_and_frames(io.BytesIO(out))
    assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES

    ds_in = Dataset()
    ds_in.parse(out, format="jelly")

    quads_in = set(ds_in)
    assert quads_in == {(*triple, DATASET_DEFAULT_GRAPH_ID) for triple in triples_in}


@pytest.mark.parametrize("stream_class", [QuadStream, GraphStream])
def test_flat_quads(stream_class: type[Stream]) -> None:
    ds_out = Dataset()
    ds_out.parse(source="tests/e2e_test_cases/quads_rdf_1_1/weather-quads.nq")

    stream = stream_class.for_rdflib()

    out = ds_out.serialize(encoding="jelly", format="jelly", stream=stream)
    quads_out = set(ds_out)
    assert len(quads_out) > 0

    options, _ = get_options_and_frames(io.BytesIO(out))
    assert options.stream_types.physical_type == stream_class.physical_type
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS

    ds_in = Dataset()
    ds_in.parse(out, format="jelly")

    quads_in = set(ds_in)
    assert quads_in == quads_out


def test_graphs() -> None:
    options = SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS)

    ds_out = Dataset()
    g1_out = Graph(identifier="foaf")
    g1_out.parse(source="tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")
    g2_out = Graph(identifier="test")
    g2_out.parse(source="tests/e2e_test_cases/triples_rdf_1_1/p2_ontology.nt")
    ds_out.add_graph(g1_out)
    ds_out.add_graph(g2_out)

    out = ds_out.serialize(options=options, encoding="jelly", format="jelly")

    options_out, _ = get_options_and_frames(io.BytesIO(out))
    assert options_out.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    assert options_out.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS

    graphs_out = sorted(ds_out.graphs(), key=len)

    ds_in = Dataset()
    ds_in.parse(out, format="jelly")

    graphs_in = sorted(ds_in.graphs(), key=len)

    for g_out, g_in in zip(graphs_out, graphs_in):
        assert len(g_out) == len(g_in)
        assert set(g_out) == set(g_in)
