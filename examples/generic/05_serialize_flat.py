from pyjelly.integrations.generic.serialize import flat_stream_to_file
from pyjelly.integrations.generic.generic_sink import *
import random


# Example generator that yields raw triples
def generate_sample_triples():
    for _ in range(10):
        yield Triple(
            IRI("http://example.com/sensor"),
            IRI("http://example.com/humidity"),
            Literal(str(random.random())),
        )


output_file = "flat_output.jelly"
print(f"Streaming triples into {output_file}…")
sample_triples = generate_sample_triples()
with open(output_file, "wb") as out_f:
    flat_stream_to_file(sample_triples, out_f)
print("All done.")
