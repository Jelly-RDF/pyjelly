from collections.abc import MutableMapping
from contextvars import ContextVar
from pathlib import Path

import pyjelly.integrations.generic.parse
import pyjelly.integrations.rdflib.parse


def test_frame_metadata_quads() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    file = Path(__file__).parent / "quads_stream_with_metadata.jelly"
    metadatas, graph_lengths = [], []
    with Path.open(file, "rb") as quad_stream:
        graphs = pyjelly.integrations.rdflib.parse.parse_jelly_grouped(
            quad_stream, frame_metadata=frame_metadata
        )
        for i, _graph in enumerate(graphs):
            metadata = frame_metadata.get()
            graph_lengths.append(len(_graph))
            metadatas.append(metadata)
            if i == 2:
                break

    assert len(metadatas) == len(graph_lengths)
    assert metadatas == [{"c": b"\x00"}, {"c": b"\x01"}, {}]
    assert graph_lengths == [33, 35, 15]


def test_frame_metadata_triples() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    file = Path(__file__).parent / "triple_stream_with_metadata.jelly"
    metadatas, graph_lengths = [], []
    with Path.open(file, "rb") as triple_stream:
        triples = pyjelly.integrations.rdflib.parse.parse_jelly_grouped(
            triple_stream, frame_metadata=frame_metadata
        )
        for _triple in triples:
            metadata = frame_metadata.get()
            metadatas.append(metadata)
            graph_lengths.append(len(_triple))

    assert len(metadatas) == len(graph_lengths)
    assert graph_lengths == [245, 55]
    assert metadatas == [{"c": b"\x00"}, {}]


def test_frame_metadata_generic_quads() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    file = Path(__file__).parent / "quads_stream_with_metadata.jelly"
    metadatas, graph_lengths = [], []
    with Path.open(file, "rb") as quad_stream:
        graphs = pyjelly.integrations.generic.parse.parse_jelly_grouped(
            quad_stream, frame_metadata=frame_metadata
        )
        for _graph in graphs:
            metadata = frame_metadata.get()
            graph_lengths.append(len(_graph))
            metadatas.append(metadata)

    assert len(metadatas) == len(graph_lengths)
    assert metadatas == [{"c": b"\x00"}, {"c": b"\x01"}, {}]
    assert graph_lengths == [33, 35, 15]


def test_frame_metadata_generic_triples() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    file = Path(__file__).parent / "triple_stream_with_metadata.jelly"
    metadatas, graph_lengths = [], []
    with Path.open(file, "rb") as triple_stream:
        triples = pyjelly.integrations.generic.parse.parse_jelly_grouped(
            triple_stream, frame_metadata=frame_metadata
        )
        for _triple in triples:
            metadata = frame_metadata.get()
            metadatas.append(metadata)
            graph_lengths.append(len(_triple))

    assert len(metadatas) == len(graph_lengths)
    assert graph_lengths == [245, 55]
    assert metadatas == [{"c": b"\x00"}, {}]
