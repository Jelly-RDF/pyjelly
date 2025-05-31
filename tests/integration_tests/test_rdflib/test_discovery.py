import io
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from rdflib import Graph
from rdflib import plugin as rdflib_plugin
from rdflib.parser import Parser as RDFLibParser
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly import options

rdflib_entrypoint_names = ("jelly", *options.MIMETYPES)
all_entrypoints = pytest.mark.parametrize("file_format", rdflib_entrypoint_names)


def clean_plugin_cache(plugin_name: str, extension_class: type[Any]) -> None:
    # RDFLib caches plugin classes, so a mock is unable to work
    # if the plugin class was already used by some other test.
    plugin = rdflib_plugin._plugins[(plugin_name, extension_class)]
    plugin._class = None


@all_entrypoints
@patch("pyjelly.integrations.rdflib.serialize.RDFLibJellySerializer")
def test_jelly_serializer_discovered(mock: MagicMock, file_format: str) -> None:
    clean_plugin_cache(file_format, RDFLibSerializer)
    try:
        graph = Graph()
        graph.serialize(format=file_format)
        mock.assert_called_once_with(graph)
        mock.return_value.serialize.assert_called_once()
    finally:
        # Don't leak through cache
        clean_plugin_cache(file_format, RDFLibSerializer)


@all_entrypoints
@patch("pyjelly.integrations.rdflib.parse.RDFLibJellyParser")
def test_jelly_parser_discovered(mock: MagicMock, file_format: str) -> None:
    clean_plugin_cache(file_format, RDFLibParser)
    try:
        graph = Graph()
        graph.parse(io.StringIO(), format=file_format)
        mock.assert_called_once_with()
        mock.return_value.parse.assert_called_once()
    finally:
        # Don't leak through cache
        clean_plugin_cache(file_format, RDFLibParser)
