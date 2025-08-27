from rdflib import Graph, Namespace, URIRef, Literal
from pyjelly.integrations.rdflib.serialize import SerializerOptions, StreamParameters

# Build a tiny graph and bind a prefix
g = Graph()
EX = Namespace("http://example.org/")
g.namespace_manager.bind("ex", EX)
g.add((EX.alice, URIRef("http://xmlns.com/foaf/0.1/name"), Literal("Alice")))

print("IN  namespaces:", dict(g.namespaces()))

# Enable namespace declarations in Jelly output
options = SerializerOptions(params=StreamParameters(namespace_declarations=True))

# Serialize with options
g.serialize("sample_test.jelly", format="jelly", options=options)

# Parse back and check namespaces
g_new = Graph()
g_new.parse("sample_test.jelly", format="jelly")
print("OUT namespaces:", dict(g_new.namespaces()))
