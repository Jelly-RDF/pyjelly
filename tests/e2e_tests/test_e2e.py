from dataclasses import replace
from itertools import product, chain
from pathlib import Path

import pytest

from pyjelly.options import StreamOptions
from tests.e2e_tests.ser_des.base_ser_des import BaseSerDes
from tests.e2e_tests.ser_des.rdflib_ser_des import RdflibSerDes


class End2EndOptionSetup:
    """Set up stream options for E2E tests"""

    test_root: Path = Path("tests/e2e_tests/test_cases/")

    def setup_ser_des(self) -> list[tuple[BaseSerDes, BaseSerDes, StreamOptions, int]]:
        """Set up test serializer, deserializer, options and frame_size."""
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

    def setup_triple_files(
        self,
    ) -> list[tuple[BaseSerDes, BaseSerDes, StreamOptions, int, Path]]:
        """Set up options for each of the test triple files"""
        test_dir: Path = self.test_root / "triples_rdf_1_1"
        files = test_dir.glob("*.nt")
        options = self.setup_ser_des()
        return list(chain(*[[(*o, f) for o in options] for f in files]))

    def setup_quad_files(
        self,
    ) -> list[tuple[BaseSerDes, BaseSerDes, StreamOptions, int, Path]]:
        """Set up options for each of the test quad files"""
        test_dir: Path = self.test_root / "quads_rdf_1_1"
        files = test_dir.glob("*.nq")
        options = self.setup_ser_des()
        return list(chain(*[[(*o, f) for o in options] for f in files]))


class TestEnd2End:
    setup = End2EndOptionSetup()

    @pytest.mark.parametrize(
        ("ser", "des", "options", "frame_size", "file"), setup.setup_triple_files()
    )
    def test_triple_files(
        self,
        ser: BaseSerDes,
        des: BaseSerDes,
        options: StreamOptions,
        frame_size: int,
        file: Path,
    ) -> None:
        nt_reader = RdflibSerDes()
        with file.open("rb") as f:
            if (in_file := f) is not None:
                triples = nt_reader.read_triples(in_file.read())
                jelly_io = ser.write_triples_jelly(triples, options, frame_size)
                new_g = des.read_triples_jelly(jelly_io)
                assert nt_reader.len_triples(triples) == des.len_triples(new_g)

    @pytest.mark.parametrize(
        ("ser", "des", "options", "frame_size", "file"), setup.setup_quad_files()
    )
    def test_quad_files(
        self,
        ser: BaseSerDes,
        des: BaseSerDes,
        options: StreamOptions,
        frame_size: int,
        file: Path,
    ) -> None:
        nq_reader = RdflibSerDes()
        with file.open("rb") as f:
            if (in_file := f) is not None:
                quads = nq_reader.read_quads(in_file.read())
                jelly_io = ser.write_quads_jelly(quads, options, frame_size)
                new_g = des.read_quads_jelly(jelly_io)
                assert nq_reader.len_quads(quads) == des.len_quads(new_g)
