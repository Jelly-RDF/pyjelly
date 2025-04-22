import io
from unittest.mock import MagicMock, patch

import pytest
from rdflib import Graph

from pyjelly import options

rdflib_entrypoint_names = ("jelly", *options.MIMETYPES)
all_entrypoints = pytest.mark.parametrize("file_format", rdflib_entrypoint_names)


@all_entrypoints
@patch("pyjelly.integrations.rdflib.serializer.RDFLibJellySerializer")
def test_jelly_serializer_discovered(mock: MagicMock, file_format: str) -> None:
    graph = Graph()
    graph.serialize(format=file_format)
    mock.assert_called_once_with(graph)
    mock.return_value.serialize.assert_called_once()


@all_entrypoints
@patch("pyjelly.integrations.rdflib.parser.RDFLibJellyParser")
def test_jelly_parser_discovered(mock: MagicMock, file_format: str) -> None:
    graph = Graph()
    graph.parse(io.StringIO(), format=file_format)
    mock.assert_called_once_with()
    mock.return_value.parse.assert_called_once()
