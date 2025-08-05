from pyjelly.integrations.generic.generic_sink import GenericStatementSink, Triple

# Create a generic sink object
generic_sink = GenericStatementSink(identifier="my_graph")

# Load triples from the Jelly file
with open("output.jelly", "rb") as in_file:
    generic_sink.parse(in_file)

# Let's inspect it statement by statement
for statement in generic_sink:
    if isinstance(statement, Triple):
        print(statement)

print("All done.")