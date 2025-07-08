from rdflib import Graph

g = Graph()
g.parse("foaf.jelly", format="jelly")

print("Parsed triples:")
for s, p, o in g:
    print(f"{s} {p} {o}")
