from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Triple
)

# Create a generic sink object, identifier is optional
generic_sink = GenericStatementSink(identifier="my_graph")

# Let's add triples one by one
generic_sink.add(
    Triple(
        IRI("http://example.com/subject"),
        IRI("http://example.com/predicate"),
        Literal("Hello", langtag="en"),
    )
)
generic_sink.add(
    Triple(
        BlankNode("B1"),
        IRI("http://example.com/hasName"),
        Literal("Bob"),
    )
)

# Write into a Jelly file
with open("output.jelly", "wb") as out_file:
    generic_sink.serialize(out_file)

print("All done.")
