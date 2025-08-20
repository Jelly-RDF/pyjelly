from typing import Any, Final, cast

import pytest

from pyjelly import jelly
from pyjelly.integrations.generic.serialize import guess_options, guess_stream
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import SerializerOptions


class _Sink:
    def __init__(self, *, is_triples_sink: bool) -> None:
        self.is_triples_sink: Final[bool] = is_triples_sink


@pytest.mark.parametrize(
    ("sink", "expected_logical"),
    [
        (_Sink(is_triples_sink=True), jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (_Sink(is_triples_sink=False), jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["triples-sink", "quads-sink"],
)
def test_defaults_generic(
    sink: Any,
    expected_logical: int,
) -> None:
    opts = guess_options(cast(Any, sink))
    assert opts.logical_type == expected_logical
    # generic supports both by default
    assert opts.params.rdf_star is True
    assert opts.params.generalized_statements is True


@pytest.mark.parametrize(
    ("sink", "logical"),
    [
        (_Sink(is_triples_sink=True), jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (_Sink(is_triples_sink=False), jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ],
    ids=["triples-override", "quads-override"],
)
def test_override_generic(
    sink: Any,
    logical: int,
) -> None:
    user_opts = SerializerOptions(
        logical_type=cast(jelly.LogicalStreamType, logical),
        params=StreamParameters(
            rdf_star=False,
            generalized_statements=False,
        ),
    )
    stream = guess_stream(user_opts, cast(Any, sink))
    assert stream.stream_types.logical_type == logical
    assert stream.options.params.rdf_star is False
    assert stream.options.params.generalized_statements is False
