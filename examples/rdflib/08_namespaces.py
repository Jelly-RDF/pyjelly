from rdflib import Graph, Literal, Namespace
from rdflib.namespace import FOAF

from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import SerializerOptions

# Build a tiny graph and bind a prefix
EX = Namespace("http://example.org/")
g = Graph()
g.bind("ex", EX)
g.add((EX.alice, FOAF.name, Literal("Alice")))

# Enable namespace declarations in Jelly output
options = SerializerOptions(params=StreamParameters(namespace_declarations=True))
g.serialize("namespaces.jelly", format="jelly", options=options)

# Parse back and check that the prefix is there
g_new = Graph()
g_new.parse("namespaces.jelly", format="jelly")
print("Prefix ex:", dict(g_new.namespaces()).get("ex"))
