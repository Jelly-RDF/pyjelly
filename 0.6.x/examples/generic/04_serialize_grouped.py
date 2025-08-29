from pyjelly.integrations.generic.generic_sink import *
from pyjelly.integrations.generic.serialize import grouped_stream_to_file
import random


# Helper function to generate a generator of graphs
def generate_sample_sinks():
    content = (
        IRI("http://example.com/sensor"),
        IRI("http://example.com/humidity"),
        IRI(f"http://example.com/{random.random()}"),
    )
    for _ in range(10):
        sink = GenericStatementSink()
        sink.add(Triple(*content))
        yield sink


output_file = "output.jelly"
print(f"Streaming graphs into {output_file}…")
with open(output_file, "wb") as out_f:
    grouped_stream_to_file(generate_sample_sinks(), out_f)
print("All done.")
