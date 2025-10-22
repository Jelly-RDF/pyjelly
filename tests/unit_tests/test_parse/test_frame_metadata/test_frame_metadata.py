import urllib.parse
import urllib.request
from collections.abc import MutableMapping
from contextvars import ContextVar
from pathlib import Path

import pyjelly.integrations.generic.parse
import pyjelly.integrations.rdflib.parse


def test_frame_metadata_quads() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    url = "https://registry.petapico.org/nanopubs.jelly"

    with urllib.request.urlopen(url) as response:  # noqa: S310
        graphs = pyjelly.integrations.rdflib.parse.parse_jelly_grouped(
            response, frame_metadata=frame_metadata
        )
        for _graph in graphs:
            metadata = frame_metadata.get()
            break

    assert len(_graph) == 33
    assert "c" in metadata
    assert metadata["c"] == b"\x01"


def test_frame_metadata_triples() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    file = Path(__file__).parent / "triple_stream_with_metadata.jelly"
    with Path.open(file, "rb") as triple_stream:
        graphs = pyjelly.integrations.rdflib.parse.parse_jelly_grouped(
            triple_stream, frame_metadata=frame_metadata
        )
        for _graph in graphs:
            metadata = frame_metadata.get()
            break

    assert len(_graph) == 245
    assert "c" in metadata
    assert metadata["c"] == b"\x00\x00"


def test_frame_metadata_generic_quads() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    url = "https://registry.petapico.org/nanopubs.jelly"

    with urllib.request.urlopen(url) as response:  # noqa: S310
        graphs = pyjelly.integrations.generic.parse.parse_jelly_grouped(
            response, frame_metadata=frame_metadata
        )
        for _graph in graphs:
            metadata = frame_metadata.get()
            break

    assert len(_graph) == 33
    assert "c" in metadata
    assert metadata["c"] == b"\x01"


def test_frame_metadata_generic_triples() -> None:
    frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar(
        "frame_metadata"
    )
    file = Path(__file__).parent / "triple_stream_with_metadata.jelly"
    with Path.open(file, "rb") as triple_stream:
        graphs = pyjelly.integrations.generic.parse.parse_jelly_grouped(
            triple_stream, frame_metadata=frame_metadata
        )
        for _graph in graphs:
            metadata = frame_metadata.get()
            break

    assert len(_graph) == 245
    assert "c" in metadata
    assert metadata["c"] == b"\x00\x00"
