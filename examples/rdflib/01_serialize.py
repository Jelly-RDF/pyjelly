from rdflib import Graph

g = Graph()
g.parse("https://www.w3.org/TR/vocab-ssn/integrated/examples/sunspots.ttl")
g.serialize(destination="sunspots.jelly", format="jelly")
print(f"Wrote {len(g)} triples to sunspots.jelly")
