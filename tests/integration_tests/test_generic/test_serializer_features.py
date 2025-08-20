import pytest
from pyjelly import jelly
from pyjelly.options import StreamParameters
from pyjelly.integrations.generic.serialize import guess_options, guess_stream
from pyjelly.integrations.generic.streams import SerializerOptions


class _Sink:
    def __init__(self, is_triples_sink: bool) -> None:
        self.is_triples_sink = is_triples_sink


@pytest.mark.parametrize(
    "sink,expected_logical",
    [
        (_Sink(True), jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (_Sink(False), jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["triples-sink", "quads-sink"],
)
def test_defaults_generic(sink, expected_logical):
    opts = guess_options(sink)
    assert opts.logical_type == expected_logical
    assert opts.params.rdf_star is True
    assert opts.params.generalized_statements is True


@pytest.mark.parametrize(
    "sink,logical",
    [
        (_Sink(True), jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (_Sink(False), jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["triples-override", "quads-override"],
)
def test_override_generic(sink, logical):
    user_opts = SerializerOptions(
        logical_type=logical,
        params=StreamParameters(
            rdf_star=False,
            generalized_statements=False,
        ),
    )
    stream = guess_stream(user_opts, sink)
    so = stream.stream_options()
    assert so.stream_types.logical_type == logical
    assert so.params.rdf_star is False
    assert so.params.generalized_statements is False
