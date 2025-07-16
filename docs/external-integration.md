# Pyjelly integration with external libraries

This section describes how to integrate useful third‑party libraries that utilize RDFLib.  
In order to proceed with the content of the document, one should be acquainted with the [getting-started.md](getting-started.md) file.

## Neo4j - rdflib

The Neo4j RDFLib library is a Python plugin that lets you use RDFLib’s API to parse RDF triples directly in a Neo4j graph database.  
Because Neo4j integrates well with the baseline RDF objects, which is also true for pyjelly, both libraries can be easily combined.  

In further steps we will exemplify the integration methods, however first, (in our pyjelly environment) we should install neo4j-rdflib:  
```
pip install rdflib-neo4j
```

For more information, one should visit the following references found on the Neo4j-rdflib original sources, which formed the base of
the following document:
    - [github.com/neo4j-labs/rdflib-neo4j](https://github.com/neo4j-labs/rdflib-neo4j)
    - [neo4j.com/labs/rdflib-neo4j/](https://neo4j.com/labs/rdflib-neo4j/)

### Configure Your AuraDB Connection

Before running the example code, make sure to provide your own credentials to the AuraDB instance listed below:
```
AURA_DB_URI
AURA_DB_USERNAME
AURA_DB_PWD
```

### Parsing data into neo4j from a jelly file

To parse data into the neo4j database from `.jelly` file use the following example:

{{ code_example('neo4j_integration/01_rdflib_neo4j_parse.py') }}

which parses the data from `.jelly` file into the user's AuraDB database.