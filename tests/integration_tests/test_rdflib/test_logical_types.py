import io
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.parse import (
    parse_jelly_flat as generic_parse_jelly_flat,
)
from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
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


@pytest.mark.skip
def test_graphs() -> None:
    # TODO(Nastya): rewrite or remove
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


def _make_flat_triples_bytes() -> bytes:
    g_out = Graph()
    g_out.parse(source="tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")
    return g_out.serialize(encoding="jelly", format="jelly")


def _make_grouped_graphs_bytes() -> bytes:
    ds_out = Dataset()
    g1 = Graph(identifier="g1")
    g1.parse(source="tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")
    g2 = Graph(identifier="g2")
    g2.parse(source="tests/e2e_test_cases/triples_rdf_1_1/p2_ontology.nt")
    ds_out.add_graph(g1)
    ds_out.add_graph(g2)

    opts = SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS)
    return ds_out.serialize(options=opts, encoding="jelly", format="jelly")


def test_grouped_strict_raises_on_flat_triples() -> None:
    out = _make_flat_triples_bytes()
    with pytest.raises(JellyConformanceError, match="expected GROUPED logical type"):
        list(generic_parse_jelly_grouped(io.BytesIO(out), logical_type_strict=True))


def test_grouped_non_strict_parses_flat_triples() -> None:
    out = _make_flat_triples_bytes()
    sinks = list(
        generic_parse_jelly_grouped(
            io.BytesIO(out),
            logical_type_strict=False,
        )
    )
    assert len(sinks) == 1
    g_check = Graph()
    g_check.parse(source="tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")
    assert len(sinks[0]) == len(g_check)


def test_flat_strict_raises_on_grouped_graphs() -> None:
    out = _make_grouped_graphs_bytes()
    with pytest.raises(JellyConformanceError, match="expected FLAT logical type"):
        list(generic_parse_jelly_flat(io.BytesIO(out), logical_type_strict=True))


def test_flat_strict_passes_on_flat_quads() -> None:
    ds_out = Dataset()
    ds_out.parse(source="tests/e2e_test_cases/quads_rdf_1_1/weather-quads.nq")

    stream = QuadStream.for_rdflib()
    out = ds_out.serialize(encoding="jelly", format="jelly", stream=stream)

    events = list(generic_parse_jelly_flat(io.BytesIO(out), logical_type_strict=True))
    assert len(events) == len(set(ds_out))


def test_generic_flat_strict_raises_when_no_stream_types() -> None:
    dummy = b"x"
    options = type("Opt", (), {"stream_types": None})()
    frames: list[object] = []
    with (
        patch(
            "pyjelly.integrations.generic.parse.get_options_and_frames",
            return_value=(options, frames),
        ),
        pytest.raises(JellyConformanceError, match="requires options.stream_types"),
    ):
        list(generic_parse_jelly_flat(io.BytesIO(dummy), logical_type_strict=True))


def test_generic_grouped_strict_raises_when_unspecified_logical() -> None:
    class ST:
        flat = False
        logical_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
        physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES

    dummy = b"x"
    options = type("Opt", (), {"stream_types": ST()})()
    frames: list[object] = []
    with (
        patch(
            "pyjelly.integrations.generic.parse.get_options_and_frames",
            return_value=(options, frames),
        ),
        pytest.raises(JellyConformanceError, match="expected GROUPED logical type"),
    ):
        list(generic_parse_jelly_grouped(io.BytesIO(dummy), logical_type_strict=True))


def test_generic_flat_not_implemented_physical_raises() -> None:
    class ST:
        flat = True
        logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        physical_type = jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED

    dummy = b"x"
    options = type("Opt", (), {"stream_types": ST()})()
    frames: list[object] = []
    with (
        patch(
            "pyjelly.integrations.generic.parse.get_options_and_frames",
            return_value=(options, frames),
        ),
        pytest.raises(NotImplementedError, match="is not supported"),
    ):
        list(generic_parse_jelly_flat(io.BytesIO(dummy), logical_type_strict=False))
