from pyjelly.integrations.rdflib.serialize import grouped_stream_to_file

from rdflib import Graph, Literal, Namespace
import random


def generate_sample_graphs():
    ex = Namespace("http://example.org/")
    for _ in range(10):
        g = Graph()
        g.add((ex.sensor, ex.temperature, Literal(random.random())))
        g.add((ex.sensor, ex.humidity, Literal(random.random())))
        yield g


output_file_name = "output.jelly"

print(f"Streaming graphs into {output_file_name}…")
sample_graphs = generate_sample_graphs()
with open(output_file_name, "wb") as out_file:
    grouped_stream_to_file(sample_graphs, out_file)
print("All done.")
