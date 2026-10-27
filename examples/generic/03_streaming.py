from pyjelly.integrations.generic.parse import parse_jelly_flat


# Helper generator that streams statements from a Jelly file path
def stream_statements(jelly_path):
    with open(jelly_path, "rb") as f:
        # Statements are read one by one, as they are needed
        yield from parse_jelly_flat(f)


# Example usage, just printing:
for statement in stream_statements("output.jelly"):
    print(statement)

print("All done.")
