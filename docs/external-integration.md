# Pyjelly integration with external libraries

This section focuses on describing integration with useful external libraries operating with RDF library.  
In order to proceed with the content of the document, one should be acquainted with the [getting-started.md](getting-started.md) file.

## Neo4j - rdflib

The Neo4j RDFLib library is a Python plugin that lets you use RDFLib’s API to store, query, and manage RDF triples directly in a Neo4j graph database.  
Because Neo4j integrats well with the baseline RDF objects, which is also true for pyjelly, both libraries can be easily combined.  

In fuerther steps we will exemplify the integration methods, however first, (in our pyjelly environment) we should installl neo4j-rdflib:  
```
pip install rdflib-neo4j
```

### Configure Your AuraDB Connection

Before running the example code, replace the placeholder values below with your own AuraDB instance credentials:
```
AURA_DB_URI      = "your_db_uri"
AURA_DB_USERNAME = "neo4j" [Optional]
AURA_DB_PWD      = "your_db_pwd"
```

### Parsing data into neo4j from jelly file

To parse data into the neo4j database from `.jelly` file use the following example:

{{ code_example('neo4j_integration/01_rdflib_neo4j_parse.py') }}

It opens a stream through a neo4j integrated parser working with RDF library, which parses the data (also from `.jelly` file) into user's database.

### Serializing grouped data from neo4j database

To effortlessly serialize the data in the grouped form into a local `.jelly` file, see the example below.

{{ code_example('neo4j_integration/02_rdflib_neo4j_serialize_grouped.py') }}

By default the data fetched from user's database is in a grouped (in a Graph object) from, thus it can be serialized through grouped serializer (just like in the { code_example('rdflib/06_serialize_grouped.py')}).
