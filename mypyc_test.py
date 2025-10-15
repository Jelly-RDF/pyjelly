import argparse, time   
from rdflib import Graph
from pyjelly.integrations.generic.generic_sink import *

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nq", required=True, help="Input nq file")
    ap.add_argument("--out", required=True, help="Output .jelly file")
    args = ap.parse_args()
    print("Started time test")

    print("Testing generic version")
    print("Started parsing")
    t0p = time.perf_counter()
    generic_sink = GenericStatementSink()
    with open(args.nq, "rb") as in_file:
        generic_sink.parse(in_file)
    elapsed_parse = time.perf_counter() - t0p
    print(f"Time to parse: {elapsed_parse:.3f}s")

    t0s = time.perf_counter()
    print("Started serializing")
    with open(args.out, "wb") as out_file:
        generic_sink.serialize(out_file)
    elapsed_serialize = time.perf_counter() - t0s
    print(f"Time to serialize: {elapsed_serialize:.3f}s")
    
    print("Testing rdflib version")
    g = Graph()
    print("Started parsing")
    t0p = time.perf_counter()
    g.parse(args.nq)
    elapsed_parse = time.perf_counter() - t0p
    print(f"Time to parse: {elapsed_parse:.3f}s")
    print("Started serialization")
    t0s = time.perf_counter()
    g.serialize(destination=args.out, format="jelly")
    elapsed_serialize = time.perf_counter() - t0s
    print(f"Serialized to: {args.out}")
    print(f"Time to serialize: {elapsed_serialize:.3f}s")

if __name__ == "__main__":
    main()
