import time
from pathlib import Path
import io
from pyjelly.integrations.generic.generic_sink import *

def mypyc_test() -> None:
    print("Started time test")

    print("Testing generic version")
    print("Started parsing")
    generic_sink = GenericStatementSink()
    data: bytes = Path("nanopub_100k.jelly").read_bytes()
    t0p = time.perf_counter()
    with io.BytesIO(data) as buf:
        generic_sink.parse(buf)
    elapsed_parse = time.perf_counter() - t0p
    print(f"Time to parse: {elapsed_parse:.3f}s")

    t0s = time.perf_counter()
    print("Started serializing")
    generic_sink.serialize(io.BytesIO())
    elapsed_serialize = time.perf_counter() - t0s
    print(f"Time to serialize: {elapsed_serialize:.3f}s")

    # print("Testing rdflib version")
    # g = Graph()
    # print("Started parsing")
    # t0p = time.perf_counter()
    # g.parse(args.nq)
    # elapsed_parse = time.perf_counter() - t0p
    # print(f"Time to parse: {elapsed_parse:.3f}s")
    # print("Started serialization")
    # t0s = time.perf_counter()
    # g.serialize(destination=args.out, format="jelly")
    # elapsed_serialize = time.perf_counter() - t0s
    # print(f"Serialized to: {args.out}")
    # print(f"Time to serialize: {elapsed_serialize:.3f}s")
    
t0 = time.time()
mypyc_test()
print(time.time() - t0)
