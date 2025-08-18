from rdflib import Graph, Namespace

EX = Namespace("http://example.org/")
g = Graph()
g.bind("ex", EX)
g.add((EX.alice, EX.knows, EX.bob))

g.serialize("out.jelly", format="jelly")
