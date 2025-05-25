import io
from operator import itemgetter

from rdflib import Dataset, Graph

from pyjelly import jelly
from pyjelly.parse.ioutils import get_options_and_frames

g_out = Graph()
g_out.parse(source="http://xmlns.com/foaf/spec/index.rdf")

out = g_out.serialize(encoding="jelly", format="jelly")
triples_out = set(g_out)
assert len(triples_out) > 0

g_in = Graph()
g_in.parse(out, format="jelly")

triples_in = set(g_in)

assert len(triples_out) == len(triples_in)
assert triples_in == triples_out

options, _ = get_options_and_frames(io.BytesIO(out))
assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES

ds_in = Dataset()
ds_in.parse(out, format="jelly")

quads_in = set(ds_in)
triples_in_from_quads = set(map(itemgetter(slice(0, 3)), quads_in))
assert triples_in_from_quads == triples_out
