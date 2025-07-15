from rdflib import Graph, Namespace, Literal
from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore, HANDLE_VOCAB_URI_STRATEGY
import random

# Auxiliary function to generate graphs
def generate_sample_graphs():
    ex = Namespace("http://example.org/")
    for _ in range(10):
        g = Graph()
        g.add((ex.sensor, ex.temperature, Literal(random.random())))
        g.add((ex.sensor, ex.humidity, Literal(random.random())))
        yield g

# Please introduce your credentials
aura_db_uri = "neo4j+s://8c296e60.databases.neo4j.io"
aura_db_username = "neo4j"
aura_db_pwd = "wP3r_U1oBXp3o5oQg_35NT3lnGGrcwXw9sNIEbvZk-I"

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
neo4j_aura_graphs = Graph(store=Neo4jStore(config=config))

# Generate a example graph
example_data_graph = generate_sample_graphs()

# You can just parse the data in to the neo4j Aura database
neo4j_aura_graphs.parse(example_data_graph)

# Close the neo4j store instance
neo4j_aura_graphs.close(True)
