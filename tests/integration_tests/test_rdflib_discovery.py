import unittest.mock

import pytest
from rdflib import Graph

from pyjelly import options


@pytest.mark.parametrize("serialize_format", options.MIMETYPES)
@unittest.mock.patch(
    "pyjelly.integrations.with_rdflib.serializer.RDFLibJellySerializer"
)
def test_jelly_serializer_discovered(
    mock: unittest.mock.MagicMock,
    serialize_format: str,
) -> None:
    graph = Graph()
    graph.serialize(format=serialize_format)
    mock.assert_called_once_with(graph)
    mock.return_value.serialize.assert_called_once()
