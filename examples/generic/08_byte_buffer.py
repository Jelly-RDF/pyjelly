import io

from pyjelly.integrations.generic.generic_sink import *

g1 = GenericStatementSink()

g1.add(
    Triple(
        IRI("http://example.com/subject"),
        IRI("http://example.com/predicate"),
        Literal("Hello", langtag="en"),
    )
)

# Write the data into a byte buffer (bytes type)
with io.BytesIO() as write_buffer:
    g1.serialize(write_buffer)
    data = write_buffer.getvalue()

print(f"Serialized data size: {len(data)} bytes")

# Parse the data back
g2 = GenericStatementSink()
with io.BytesIO(data) as read_buffer:
    g2.parse(read_buffer)

print("\nParsed triples:")
for statement in g2:
    print(statement)
