# RDFLib-Neo4j

The RDFLib-Neo4j library is a Python plugin that lets you use RDFLib’s API to parse RDF triples directly into a Neo4j graph database.  
Because Neo4j integrates well with the baseline RDFLib objects, which is also true for pyjelly, you can easily use both libraries.  

Install the following library:  

```bash
pip install rdflib-neo4j
```

For more information, visit the following references from the RDFLib-Neo4j original sources:

- [RDFLib-Neo4j (GitHub)](https://github.com/neo4j-labs/rdflib-neo4j)
- [Neo4j Labs: RDFLib-Neo4j](https://neo4j.com/labs/rdflib-neo4j/)

## Parsing data from a Jelly file into Neo4j

To parse data from a `.jelly` file into the Neo4j database, use the following example with your own credentials to AuraDB:

{{ code_example('neo4j_integration/01_rdflib_neo4j_parse_grouped.py') }}

which parses the data from a `.jelly` file into your AuraDB database.