from pyjelly.integrations.generic.generic_sink import *


# Helper generator that streams statements from a Jelly file path
def stream_triples(jelly_path):
    generic_sink = GenericStatementSink()
    with open(jelly_path, "rb") as f:
        generic_sink.parse(f)
    yield from (stmt for stmt in generic_sink)


# Example usage, just printing:
for triple in stream_triples("output.jelly"):
    print(triple)

print("All done.")
