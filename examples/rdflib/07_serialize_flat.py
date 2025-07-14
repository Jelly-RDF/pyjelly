from pyjelly.integrations.rdflib.serialize import flat_stream_to_file
from rdflib import Literal, Namespace
import random


# example generator with triples statements
def generate_sample_triples():
    ex = Namespace("http://example.org/")
    for _ in range(10):
        yield (ex.sensor, ex.temperature, Literal(random.random()))


output_file_name = "flat_output.jelly"

print(f"Streaming triples into {output_file_name}…")
sample_triples = generate_sample_triples()
with open(output_file_name, "wb") as out_file:
    flat_stream_to_file(sample_triples, out_file)
print("All done.")
