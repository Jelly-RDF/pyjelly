from __future__ import annotations

import io

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.decode import GenericStatementSinkAdapter
from pyjelly.integrations.generic.generic_sink import IRI
from pyjelly.parse.sparql import SparqlDecoder, read_result_set
from pyjelly.serialize.ioutils import write_delimited, write_single

OPTIONS = jelly.SparqlResultsOptions(max_name_table_size=128, version=1)


def _decoder(**kwargs: int) -> SparqlDecoder:
    return SparqlDecoder(GenericStatementSinkAdapter, **kwargs)


def _frame(**fields: object) -> jelly.SparqlResultsFrame:
    return jelly.SparqlResultsFrame(options=OPTIONS, **fields)  # type: ignore[arg-type]


def test_row_limit() -> None:
    decoder = _decoder(max_rows=10)
    with pytest.raises(JellyConformanceError, match="more than the 10 allowed"):
        decoder.decode(_frame(variables=["x"], row_count=11))


def test_value_limit() -> None:
    # Checked before the cells are made
    decoder = _decoder(max_values=100)
    frame = _frame(variables=["x", "y"], row_count=51)
    with pytest.raises(JellyConformanceError, match="100 values allowed"):
        decoder.decode(frame)


def test_zero_variables() -> None:
    decoded = _decoder().decode(_frame(row_count=3))
    assert decoded.variables == []
    assert decoded.rows == [(), (), ()]


def test_column_padded_with_unbound_cells() -> None:
    column = jelly.RdfColumn(name_ids=[1])
    frame = _frame(
        variables=["x"],
        row_count=3,
        names=[jelly.RdfLookupEntryPacked(values=["http://example.org/a"])],
        columns=[column],
    )
    decoded = _decoder().decode(frame)
    assert decoded.rows == [(IRI("http://example.org/a"),), (None,), (None,)]


def test_links_not_utf8_are_ignored() -> None:
    decoded = _decoder().decode(_frame(metadata={"link": b"\xff"}))
    assert decoded.links is None


@pytest.mark.parametrize("delimited", [True, False])
def test_read_delimited_or_not(*, delimited: bool) -> None:
    out = io.BytesIO()
    frame = _frame(variables=["x"], trailer=jelly.SparqlResultsTrailer())
    if delimited:
        write_delimited(frame, out)
    else:
        write_single(frame, out)
    out.seek(0)
    results = read_result_set(out, GenericStatementSinkAdapter)
    assert results.variables == ["x"]
    assert list(results) == []
    assert not results.truncated
