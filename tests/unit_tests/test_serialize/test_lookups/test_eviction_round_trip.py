"""
Round trips with small lookup tables, so that entries are evicted all the time.

Checks that eviction never replaces an entry that the statement being written still
refers to, and that the indices it picks are written correctly. The prefix table is
the size of the most prefixes one quad can use (4), so that an entry used early in
a statement can already count as old by its end. In Jelly-RDF 1.2, all entries of
a frame must fit in the lookups, so frames end early.
"""

from __future__ import annotations

import io
import random

import pytest
from rdflib import Dataset, Literal, URIRef

from pyjelly import jelly
from pyjelly.integrations.generic import generic_sink as generic
from pyjelly.integrations.generic.parse import parse_jelly_flat
from pyjelly.integrations.generic.serialize import flat_stream_to_file
from pyjelly.options import (
    MIN_COLUMN_NAME_LOOKUP_SIZE,
    VERSION_1_1,
    VERSION_1_2,
    LookupPreset,
    StreamParameters,
)
from pyjelly.serialize.streams import SerializerOptions

SMALL = LookupPreset(max_names=8, max_prefixes=4, max_datatypes=8)
# The smallest name lookup of Jelly-RDF 1.2: the IRIs have 200 names
SMALL_1_2 = LookupPreset(
    max_names=MIN_COLUMN_NAME_LOOKUP_SIZE, max_prefixes=4, max_datatypes=8
)
VERSIONS = pytest.mark.parametrize(
    ("version", "preset"), [(VERSION_1_1, SMALL), (VERSION_1_2, SMALL_1_2)]
)

# (subject, predicate, object, graph). The object is (IRI, "") or a literal as
# (lexical form, datatype IRI or None).
PlainQuad = tuple[str, str, tuple[str, str | None], str]


def make_quads(count: int) -> list[PlainQuad]:
    """Return quads as plain strings: many more names and prefixes than fit."""
    rng = random.Random(42)

    def iri() -> str:
        return f"http://example.org/ns{rng.randrange(30)}/name{rng.randrange(200)}"

    quads: list[PlainQuad] = []
    for _ in range(count):
        kind = rng.random()
        if kind < 0.5:
            obj: tuple[str, str | None] = (iri(), "")
        else:
            datatype = (
                f"http://example.org/dt{rng.randrange(20)}" if kind < 0.75 else None
            )
            obj = (str(rng.randrange(1000)), datatype)
        quads.append((iri(), iri(), obj, f"http://example.org/g{rng.randrange(10)}"))
    return quads


@VERSIONS
def test_rdflib_round_trip_with_small_lookups(
    version: int, preset: LookupPreset
) -> None:
    dataset = Dataset()
    for s, p, (value, datatype), g in make_quads(3000):
        o = URIRef(value) if datatype == "" else Literal(value, datatype=datatype)
        dataset.graph(URIRef(g)).add((URIRef(s), URIRef(p), o))

    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(version=version),
        lookup_preset=preset,
    )
    out = io.BytesIO()
    dataset.serialize(destination=out, format="jelly", options=options)
    read = Dataset()
    read.parse(data=out.getvalue(), format="jelly")

    expected = set(dataset.quads((None, None, None, None)))
    assert set(read.quads((None, None, None, None))) == expected


@VERSIONS
def test_generic_round_trip_with_small_lookups(
    version: int, preset: LookupPreset
) -> None:
    quads = [
        generic.Quad(
            generic.IRI(s),
            generic.IRI(p),
            generic.IRI(value)
            if datatype == ""
            else generic.Literal(value, datatype=datatype),
            generic.IRI(g),
        )
        for s, p, (value, datatype), g in make_quads(3000)
    ]

    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(version=version),
        lookup_preset=preset,
    )
    out = io.BytesIO()
    flat_stream_to_file((quad for quad in quads), out, options)
    read = list(parse_jelly_flat(io.BytesIO(out.getvalue())))

    # The generic writer keeps the order, so the statements must come back as given
    assert read == quads
