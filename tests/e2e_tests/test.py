import io
import logging
from pathlib import Path

import pytest
from rdflib import Graph

from pyjelly.options import StreamOptions
from tests.e2e_tests.ser_des.base_ser_des import BaseSerDes
from tests.e2e_tests.ser_des.rdflib_ser_des import RdflibSerDes
from tests.utils.ordered_memory import OrderedMemory


def check_triple_files(ser: BaseSerDes, des: BaseSerDes, options: StreamOptions, test_dir: Path) -> None:
    nt_reader = RdflibSerDes()
    for file in test_dir.glob("*.nt"):
        with file.open("rb") as f:
            triples = nt_reader.read_triples(io.BytesIO(f.read()))
            jelly_io = ser.write_triples_jelly(triples, options)
            new_g = des.read_triples_jelly(jelly_io)
            assert len(triples) == len(new_g)

def check_quad_files(ser: BaseSerDes, des: BaseSerDes, options: StreamOptions, test_dir: Path) -> None:
    nq_reader = RdflibSerDes()
    for file in test_dir.glob("*.nq"):
        with file.open("rb") as f:
            quads = nq_reader.read_quads(io.BytesIO(f.read()))
            jelly_io = ser.write_quads_jelly(quads, options)
            new_g = des.read_quads_jelly(jelly_io)
            assert len(quads) == len(new_g)
   

def run_test(ser: BaseSerDes, des: BaseSerDes, options: StreamOptions) -> None:
    test_root: Path = Path("tests/e2e_tests/test_cases/")
    check_triple_files(ser, des, options, test_root.joinpath("triples_rdf_1_1"))
    #check_quad_files(ser, des, options, test_root.joinpath("quads_rdf_1_1"))
    
if __name__ == "__main__":
    run_test(RdflibSerDes(), RdflibSerDes(), StreamOptions.small())