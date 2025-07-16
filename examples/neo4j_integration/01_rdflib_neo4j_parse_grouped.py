import urllib.request
from neo4j import GraphDatabase
from rdflib import Graph
from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore, HANDLE_VOCAB_URI_STRATEGY

# Example file from the Riverbench
example_file, _ = urllib.request.urlretrieve("https://w3id.org/riverbench/v/dev.jelly")

# Please introduce your credentials
aura_db_uri = "aura_db_uri"
aura_db_username = "neo4j"
aura_db_pwd = "aura_db_pwd"

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
neo4j_sink = Graph(store=Neo4jStore(config=config))

# Parse the file into the AuraDB
neo4j_sink.parse(example_file, format="jelly")

# Close the stream
neo4j_sink.close(True)

# Cypher query to check loaded data
with GraphDatabase.driver(
    aura_db_uri, auth=(aura_db_username, aura_db_pwd)
).session() as session:
    count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    print(f"Loaded {count} triples")
    for r in session.run(
        "MATCH (s)-[r]->(o) RETURN s.uri AS s, type(r) AS p, coalesce(o.uri,o.name) AS o LIMIT 5"
    ):
        print(r["s"], f"-[{r['p']}]->", r["o"])

print("All done.")
