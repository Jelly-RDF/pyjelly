from pyjelly.integrations.rdflib.serialize import (
    triples_stream_frames,
    serialise_stream_grouped,
    serialize_save,
)

# set up an output to save jelly file
OUTPUT = "streamed_output.jelly"

print(f"Streaming triples into {OUTPUT}…")
serialize_save(serialise_stream_grouped, OUTPUT, triples_stream_frames)
print(f"Wrote a stream of example graphs into {OUTPUT} successfuly!\nAll done!")
