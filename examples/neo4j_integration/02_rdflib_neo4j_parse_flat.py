from rdflib import Graph
from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore, HANDLE_VOCAB_URI_STRATEGY

# Please introduce your credentials
aura_db_uri = "your_db_uri"
aura_db_username = "neo4j"
aura_db_pwd = "your_db_pwd"

# Prepare the authentication data to the Aura database
auth_data = {
    "uri": aura_db_uri,
    "database": "neo4j",
    "user": aura_db_username,
    "pwd": aura_db_pwd,
}

config = Neo4jStoreConfig(
    auth_data=auth_data,
    handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
    batching=True,
)

# Open the graph to stream to the AuraDB database
flat_data = Graph(store=Neo4jStore(config=config))
flat_data.open(config)

# Load example triples from an example .jelly file to parse through our stream
example_graph = Graph()
example_graph.parse("foaf.jelly", format="jelly")

# Add triple by triple to AuraDB
for subj, pred, obj in example_graph:
    flat_data.add((subj, pred, obj))

# Close the graph object
flat_data.close(True)
print("All done.")
