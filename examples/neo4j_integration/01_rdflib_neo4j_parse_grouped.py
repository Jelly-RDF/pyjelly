from rdflib import Graph
from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore, HANDLE_VOCAB_URI_STRATEGY

example_file = "foaf.jelly"

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

# Prepare the configuration for neo4j store object
config = Neo4jStoreConfig(
    auth_data=auth_data,
    handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
    batching=True,
)

# Make a graph with Neo4jStore object
Graph_data = Graph(store=Neo4jStore(config=config))

# Your reference to data in rdf compatible format in batches
data_grouped_example = [example_file for _ in range(10)]

for idx, data in enumerate(data_grouped_example):
    Graph_data.parse(data, format="jelly")
    print(f"File with index {idx} has been successfuly parsed.")
Graph_data.close(True)
print("All done.")
