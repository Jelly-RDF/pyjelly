from rdflib import Graph
from pyjelly.integrations.rdflib.serialize import SerializerOptions, StreamParameters

g = Graph()
g.parse("sample.ttl", format="turtle")
print("IN  namespaces:", dict(g.namespaces()))

# Create custom options with namespace declarations enabled
options = SerializerOptions(
    params=StreamParameters(
        generalized_statements=False,
        rdf_star=False,
        namespace_declarations=True,  # ← ENABLE NAMESPACE DECLARATIONS IN OUTPUT
    )
)

# Pass options to serialize method
g.serialize("sample_test.jelly", format="jelly", options=options)

g_new = Graph()
g_new.parse("sample_test.jelly", format="jelly")
print("OUT namespaces:", dict(g_new.namespaces()))
