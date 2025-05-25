import io

from rdflib import Dataset

from pyjelly import jelly
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.streams import GraphStream

ds_out = Dataset()
ds_out.parse(source="tests/e2e_test_cases/quads_rdf_1_1/weather-quads.nq")

stream = GraphStream.for_rdflib()

out = ds_out.serialize(encoding="jelly", format="jelly", stream=stream)
quads_out = set(ds_out)
assert len(quads_out) > 0

options, _ = get_options_and_frames(io.BytesIO(out))
assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_GRAPHS
assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS

ds_in = Dataset()
ds_in.parse(out, format="jelly")

quads_in = set(ds_in)
assert quads_in == quads_out
