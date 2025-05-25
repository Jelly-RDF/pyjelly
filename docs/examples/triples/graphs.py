import io

from rdflib import Dataset, Graph

from pyjelly import jelly
from pyjelly.integrations.rdflib.serialize import SerializerOptions
from pyjelly.parse.ioutils import get_options_and_frames

options = SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS)

ds_out = Dataset()
g1_out = Graph(identifier="foaf")
g1_out.parse(source="http://xmlns.com/foaf/spec/index.rdf")
g2_out = Graph(identifier="test")
g2_out.parse(source="https://www.w3.org/2000/10/rdf-tests/rdfcore/ntriples/test.nt")
ds_out.add_graph(g1_out)
ds_out.add_graph(g2_out)

out = ds_out.serialize(options=options, encoding="jelly", format="jelly")

options, _ = get_options_and_frames(io.BytesIO(out))
assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS

graphs_out = sorted(ds_out.graphs(), key=len)

ds_in = Dataset()
ds_in.parse(out, format="jelly")

graphs_in = sorted(ds_in.graphs(), key=len)

assert len(graphs_in) == 2 + 1  # +1 for default graph
assert len(graphs_out) == 2 + 1  # +1 for default graph

for g_out, g_in in zip(graphs_out, graphs_in):
    assert len(g_out) == len(g_in)
    assert set(g_out) == set(g_in)
