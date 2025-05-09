import io
import logging
from pathlib import Path

import pytest
from rdflib import Graph
from pyjelly.options import StreamOptions
from tests.e2e_tests.ser_des.base_ser_des import BaseSerDes
from tests.e2e_tests.ser_des.rdflib_ser_des import RdflibSerDes
from tests.utils.ordered_memory import OrderedMemory

def setup_ser_des():
    ser = RdflibSerDes()
    des = RdflibSerDes()
    options = StreamOptions.small()
    return ser, des, options

class TestEnd2End:
    
    test_root: Path = Path("tests/e2e_tests/test_cases/")

    @pytest.mark.parametrize("ser, des, options", [setup_ser_des()])
    def test_triple_files(self, ser, des, options, subtests) -> None:
        nt_reader = RdflibSerDes()
        test_dir: Path = self.test_root.joinpath("triples_rdf_1_1")
        for file in test_dir.glob("*.nt"):
            with subtests.test(msg=f"Testing file: {file}"):
                with file.open("rb") as f:
                    triples = nt_reader.read_triples(io.BytesIO(f.read()))
                    jelly_io = ser.write_triples_jelly(triples, options)
                    new_g = des.read_triples_jelly(jelly_io)
                    assert nt_reader.len_triples(triples) == des.len_triples(new_g)

    @pytest.mark.parametrize("ser, des, options", [setup_ser_des()])
    def test_quad_files(self, ser: BaseSerDes, des: BaseSerDes, options: StreamOptions, subtests) -> None:
        nq_reader = RdflibSerDes()
        test_dir: Path = self.test_root.joinpath("quads_rdf_1_1")
        for file in test_dir.glob("*.nq"):
            with subtests.test(msg=f"Testing file: {file}"):
                with file.open("rb") as f:
                    quads = nq_reader.read_quads(io.BytesIO(f.read()))
                    jelly_io = ser.write_quads_jelly(quads, options, frame_size=100)
                    new_g = des.read_quads_jelly(jelly_io)
                    assert nq_reader.len_quads(quads) == des.len_quads(new_g)