from typing import Any, Final, cast

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError
from pyjelly.integrations.generic.serialize import guess_options, guess_stream
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import QuadStream, SerializerOptions, TripleStream


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
    assert opts.params.rdf_star is True
    assert opts.params.generalized_statements is True
    assert opts.params.namespace_declarations is False


@pytest.mark.parametrize(
    ("sink", "logical", "expected_physical"),
    [
        (
            _Sink(is_triples_sink=True),
            jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            TripleStream,
        ),
        (
            _Sink(is_triples_sink=False),
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            QuadStream,
        ),
    ],
    ids=["triples-sink+triples", "quads-sink+quads"],
)
def test_override_generic_compatible(
    sink: Any,
    logical: int,
    expected_physical: type,
) -> None:
    user_opts = SerializerOptions(
        logical_type=cast(jelly.LogicalStreamType, logical),
        params=StreamParameters(
            rdf_star=False, generalized_statements=False, namespace_declarations=False
        ),
    )
    stream = guess_stream(user_opts, cast(Any, sink))
    assert isinstance(stream, expected_physical)
    assert stream.options.logical_type == logical
    assert stream.options.params.rdf_star is False
    assert stream.options.params.generalized_statements is False


@pytest.mark.parametrize(
    ("sink", "logical"),
    [
        (_Sink(is_triples_sink=True), jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
        (_Sink(is_triples_sink=False), jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
    ],
    ids=["triples-sink+quads-error", "quads-sink+triples-error"],
)
def test_override_generic_incompatible(
    sink: Any,
    logical: int,
) -> None:
    user_opts = SerializerOptions(
        logical_type=cast(jelly.LogicalStreamType, logical),
        params=StreamParameters(
            rdf_star=False, generalized_statements=False, namespace_declarations=False
        ),
    )
    with pytest.raises(
        JellyAssertionError,
        match="is not compatible with",
    ):
        guess_stream(user_opts, cast(Any, sink))
