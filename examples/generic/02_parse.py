from pyjelly.integrations.generic.generic_sink import *

# Create a generic sink object, identifier is optional
generic_sink = GenericStatementSink(identifier="my_graph")

# Load triples from the Jelly file
with open("output.jelly", "rb") as in_file:
    generic_sink.parse(in_file)

# Let's inspect them statement by statement
for statement in generic_sink:
    if isinstance(statement, Triple):
        print(statement)

print("All done.")
