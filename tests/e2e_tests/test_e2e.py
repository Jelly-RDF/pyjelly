import io
from dataclasses import replace
from itertools import product
from pathlib import Path

import pytest
from pytest_subtests import SubTests

from pyjelly.options import StreamOptions
from tests.e2e_tests.ser_des.base_ser_des import BaseSerDes
from tests.e2e_tests.ser_des.rdflib_ser_des import RdflibSerDes


def setup_ser_des() -> list[tuple[BaseSerDes, BaseSerDes, StreamOptions, int]]:
    """Set up the tests."""
    ser = [RdflibSerDes()]
    des = [RdflibSerDes()]
    # We want to have a variety of options to test
    # Particularly examples of small lookup sizes
    # and a lack of prefix
    small_no_pref = replace(StreamOptions.small(), prefix_lookup_size=0)
    tiny_tiny_pred = replace(
        StreamOptions.small(), name_lookup_size=16, prefix_lookup_size=8
    )
    options = [
        StreamOptions.small(),
        small_no_pref,
        tiny_tiny_pred,
        StreamOptions.big(),
    ]
    file_sizes = [1, 4, 200, 10_000]
    return list(product(ser, des, options, file_sizes))


class TestEnd2End:
    test_root: Path = Path("tests/e2e_tests/test_cases/")

    @pytest.mark.parametrize(("ser", "des", "options", "frame_size"), setup_ser_des())
    def test_triple_files(
        self,
        ser: BaseSerDes,
        des: BaseSerDes,
        options: StreamOptions,
        frame_size: int,
        subtests: SubTests,
    ) -> None:
        nt_reader = RdflibSerDes()
        test_dir: Path = self.test_root.joinpath("triples_rdf_1_1")
        for file in test_dir.glob("*.nt"):
            with subtests.test(msg=f"Testing file: {file}") and file.open("rb") as f:
                if (in_file := f) is not None:
                    triples = nt_reader.read_triples(io.BytesIO(in_file.read()))
                    jelly_io = ser.write_triples_jelly(triples, options, frame_size)
                    new_g = des.read_triples_jelly(jelly_io)
                    assert nt_reader.len_triples(triples) == des.len_triples(new_g)

    @pytest.mark.parametrize(("ser", "des", "options", "frame_size"), setup_ser_des())
    def test_quad_files(
        self,
        ser: BaseSerDes,
        des: BaseSerDes,
        options: StreamOptions,
        frame_size: int,
        subtests: SubTests,
    ) -> None:
        nq_reader = RdflibSerDes()
        test_dir: Path = self.test_root.joinpath("quads_rdf_1_1")
        for file in test_dir.glob("*.nq"):
            with subtests.test(msg=f"Testing file: {file}") and file.open("rb") as f:
                if (in_file := f) is not None:
                    quads = nq_reader.read_quads(io.BytesIO(in_file.read()))
                    jelly_io = ser.write_quads_jelly(quads, options, frame_size)
                    new_g = des.read_quads_jelly(jelly_io)
                    assert nq_reader.len_quads(quads) == des.len_quads(new_g)
