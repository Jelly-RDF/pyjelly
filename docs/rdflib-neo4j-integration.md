# RDFLib-Neo4j

!!! info

    This is an integration with Neo4j via the [RDFLib-Neo4j](https://neo4j.com/labs/rdflib-neo4j/) library. It works with all Neo4j instances, including AuraDB, but is limited in features and performance. If you are self-hosting Neo4j (Neo4j Desktop or Community/Enterprise), consider using the **[Jelly-Neo4j plugin for neosemantics](https://w3id.org/jelly/jelly-jvm/dev/getting-started-neo4j/)**, which offers better performance and more features.

RDFLib-Neo4j lets you import RDF triples into Neo4j via the RDFLib API.

Install the following library:  

```bash
pip install pyjelly[rdflib] rdflib-neo4j
```

## Parsing data from a Jelly file into Neo4j

To parse data from a `.jelly` file into the Neo4j database, use the following example (insert your own credentials to AuraDB):

{{ snippet_admonition('examples/neo4j_integration/01_rdflib_neo4j_parse_grouped.py',0, 50, title="Parsing", expanded=True) }}

which inserts your data into your AuraDB database.

## Related sources

For more information, visit:

- [RDFLib-Neo4j (GitHub)](https://github.com/neo4j-labs/rdflib-neo4j)
- [Neo4j Labs: RDFLib-Neo4j](https://neo4j.com/labs/rdflib-neo4j/)
- [Alternative: Jelly-Neo4j plugin for neosemantics](https://w3id.org/jelly/jelly-jvm/dev/getting-started-neo4j/)
