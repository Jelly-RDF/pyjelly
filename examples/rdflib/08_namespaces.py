from rdflib import Graph

g = Graph()
g.parse("sample.ttl", format="turtle")
print("IN  namespaces:", dict(g.namespaces()))

g.serialize("sample_test.jelly", format="jelly")

g_new = Graph()
g_new.parse("sample_test.jelly", format="jelly")
print("OUT namespaces:", dict(g_new.namespaces()))
