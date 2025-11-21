from rdflib import Graph

g = Graph()
g.parse("https://www.w3.org/TR/vocab-ssn/integrated/examples/sunspots.ttl")
g.serialize(destination="foaf.jelly", format="jelly")
