from rdflib import Graph
from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore, HANDLE_VOCAB_URI_STRATEGY

# Please introduce your credentials
AURA_DB_URI = "your_db_uri"
AURA_DB_USERNAME = "neo4j"
AURA_DB_PWD = "your_db_pwd"

# Prepare the authentication data to the Aura database
auth_data = {
    "uri": AURA_DB_URI,
    "database": "neo4j",
    "user": AURA_DB_USERNAME,
    "pwd": AURA_DB_PWD,
}

# Prepare the configuration for neo4j store object
config = Neo4jStoreConfig(
    auth_data=auth_data,
    handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
    batching=True,
)
neo4j_aura_graphs = Graph(store=Neo4jStore(config=config))

# You can just parse the data in to the neo4j Aura database
neo4j_aura_graphs.parse("foaf.jelly", format="jelly")

# Close the neo4j store instance
neo4j_aura_graphs.close(True)
