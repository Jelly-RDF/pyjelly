from rdflib import Graph

from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore, HANDLE_VOCAB_URI_STRATEGY
from pyjelly.integrations.rdflib.serialize import grouped_stream_to_file

OUTPUT = "neo4j_output.jelly"
URL = "https://www.wikidata.org/wiki/Special:EntityData/Q181191.ttl"

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

# Parse the example data from a wikidata article
neo4j_aura_graphs.parse(URL, format="ttl")

# Serialization step with grouped stream to file
with open(OUTPUT, "wb") as out_file:
    grouped_stream_to_file(neo4j_aura_graphs, out_file)
neo4j_aura_graphs.close(True)
