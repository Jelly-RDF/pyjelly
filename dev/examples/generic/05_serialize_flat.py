from pyjelly.integrations.generic.serialize import flat_stream_to_file
from pyjelly.integrations.generic.generic_sink import *
import random


# Example generator that yields raw triples
def generate_sample_triples():
    content = (
        IRI("http://example.com/sensor"),
        IRI("http://example.com/humidity"),
        IRI(f"http://example.com/{random.random()}"),
    )
    for _ in range(10):
        yield Triple(*content)


output_file = "flat_output.jelly"
print(f"Streaming triples into {output_file}…")
sample_triples = generate_sample_triples()
with open(output_file, "wb") as out_f:
    flat_stream_to_file(sample_triples, out_f)
print("All done.")
