from kgtk.io.kgtkreader import KgtkReader

graph = KgtkReader.open("foaf.jelly")

print("Parsed triples:")
for s, p, o in graph:
    print(s, p, o)

print("All done.")