from __future__ import annotations

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.generic_sink import IRI, Literal
from pyjelly.integrations.generic.serialize import GenericSinkTermEncoder
from pyjelly.integrations.generic.sparql import sparql_stream
from pyjelly.options import LookupPreset, SparqlOptions
from pyjelly.serialize.sparql import SparqlStream, links_value

A = IRI("http://example.org/a")


def test_variable_names() -> None:
    stream = sparql_stream()
    stream.start(["?s", "$p", "o"])
    frame = stream.end()
    assert list(frame.variables) == ["s", "p", "o"]


@pytest.mark.parametrize("name", ["", "?", "$"])
def test_empty_variable_name(name: str) -> None:
    with pytest.raises(JellyConformanceError, match="cannot be empty"):
        sparql_stream().start([name])


def test_first_frame() -> None:
    stream = sparql_stream(SparqlOptions(stream_name="s"))
    stream.start(["x"], links=["http://example.org/l1", "http://example.org/l2"])
    stream.row([A])
    frame = stream.end()
    assert frame.options == jelly.SparqlResultsOptions(
        stream_name="s",
        max_name_table_size=8192,
        max_prefix_table_size=1024,
        max_datatype_table_size=64,
        version=1,
    )
    assert frame.metadata["link"] == b"http://example.org/l1\nhttp://example.org/l2"
    assert frame.row_count == 1
    assert frame.HasField("trailer")
    assert not frame.trailer.error


def test_frame_without_rows_has_no_columns() -> None:
    stream = sparql_stream()
    stream.start(["x", "y"])
    frame = stream.end()
    assert frame.row_count == 0
    assert not frame.columns


def test_unbound_column_is_an_empty_message() -> None:
    stream = sparql_stream()
    stream.start(["x", "y"])
    stream.row([A, None])
    frame = stream.end()
    assert len(frame.columns) == 2
    assert frame.columns[1] == jelly.RdfColumn()


def test_row_length() -> None:
    stream = sparql_stream()
    stream.start(["x", "y"])
    with pytest.raises(JellyConformanceError, match="expected 2 values"):
        stream.row([A])


def test_order_of_calls() -> None:
    stream = sparql_stream()
    with pytest.raises(JellyConformanceError, match="start a result set"):
        stream.row([A])
    with pytest.raises(JellyConformanceError, match="no result set to end"):
        stream.end()
    stream.start(["x"])
    with pytest.raises(JellyConformanceError, match="not ended yet"):
        stream.start(["x"])
    with pytest.raises(JellyConformanceError, match="not ended yet"):
        stream.ask(True)  # noqa: FBT003
    stream.end()
    # A FLAT stream has one result set
    with pytest.raises(JellyConformanceError, match="punctuated=True"):
        stream.start(["x"])
    with pytest.raises(JellyConformanceError, match="punctuated=True"):
        stream.ask(True)  # noqa: FBT003


def test_failed_row() -> None:
    stream = sparql_stream(
        SparqlOptions(lookup_preset=LookupPreset(max_names=128, max_datatypes=0))
    )
    stream.start(["x"])
    stream.row([A])
    with pytest.raises(JellyConformanceError, match="datatype lookup"):
        stream.row([Literal("1", datatype="http://example.org/type")])
    with pytest.raises(JellyConformanceError, match="failed to encode"):
        stream.row([A])
    with pytest.raises(JellyConformanceError, match="failed to encode"):
        stream.end()
    frame = stream.end("the row failed")
    # The rows of the frame are dropped
    assert frame.row_count == 0
    assert frame.HasField("options")
    assert list(frame.variables) == ["x"]
    assert frame.trailer.error == "the row failed"
    with pytest.raises(JellyConformanceError, match="failed to encode"):
        stream.end("again")


def test_punctuated() -> None:
    stream = sparql_stream(SparqlOptions(punctuated=True))
    first = stream.ask(True)  # noqa: FBT003
    assert first.HasField("options")
    assert first.options.stream_type == jelly.SPARQL_STREAM_TYPE_PUNCTUATED
    stream.start(["x"])
    stream.row([A])
    second = stream.end()
    assert not second.HasField("options")
    assert list(second.variables) == ["x"]
    third = stream.ask(False)  # noqa: FBT003
    assert third.HasField("trailer")
    assert not third.ask_result.value


def test_punctuated_must_be_delimited() -> None:
    with pytest.raises(JellyConformanceError, match="delimited"):
        SparqlOptions(punctuated=True, delimited=False)


def test_options_checks() -> None:
    with pytest.raises(JellyConformanceError, match="at least 128"):
        SparqlOptions(lookup_preset=LookupPreset(max_names=64))
    with pytest.raises(JellyConformanceError, match="frame size"):
        SparqlOptions(frame_size=0)


def test_encoder_lookups_must_match() -> None:
    encoder = GenericSinkTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(ValueError, match="lookup sizes"):
        SparqlStream(encoder, SparqlOptions())


def test_links_with_lf() -> None:
    with pytest.raises(JellyConformanceError, match="LF"):
        links_value(["http://example.org/a\nb"])
