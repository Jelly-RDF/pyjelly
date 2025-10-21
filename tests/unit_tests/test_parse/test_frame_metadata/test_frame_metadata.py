import urllib.parse
import urllib.request
from contextvars import ContextVar

from google.protobuf.internal.containers import ScalarMap

import pyjelly.integrations.generic.parse
import pyjelly.integrations.rdflib.parse


def test_frame_metadata() -> None:
    frame_metadata: ContextVar[ScalarMap[str, bytes]] = ContextVar("frame_metadata")
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


def test_frame_metadata_generic() -> None:
    frame_metadata: ContextVar[ScalarMap[str, bytes]] = ContextVar("frame_metadata")
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
