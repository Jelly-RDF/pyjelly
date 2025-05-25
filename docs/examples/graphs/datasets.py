import io

import pytest
from rdflib import Dataset

from pyjelly import jelly
from pyjelly.integrations.rdflib.parse import datasets_from_jelly
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.streams import GraphStream, SerializerOptions

ds1_out = Dataset()
ds1_out.parse(source="tests/e2e_test_cases/quads_rdf_1_1/weather-quads.nq")

ds2_out = Dataset()
ds2_out.parse(source="tests/e2e_test_cases/quads_rdf_1_1/nanopub-rdf-stax.nq")

options = SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_DATASETS)
stream = GraphStream.for_rdflib(options=options)

out = io.BytesIO()
out.write(ds1_out.serialize(stream=stream, encoding="jelly", format="jelly"))
out.write(ds2_out.serialize(stream=stream, encoding="jelly", format="jelly"))

out.seek(0)

with pytest.raises(
    NotImplementedError,
    match="the stream contains multiple datasets and cannot be parsed into a single dataset",
):
    ds_in = Dataset()
    ds_in.parse(out, format="jelly")

out.seek(0)

options, _ = get_options_and_frames(out)
assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_GRAPHS
assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_DATASETS

out.seek(0)

datasets = tuple(datasets_from_jelly(out))

assert len(datasets) == 2

ds1_in, ds2_in = sorted(datasets, key=len)

assert set(ds1_in) == set(ds1_out)
assert set(ds2_in) == set(ds2_out)
