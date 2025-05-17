from dataclasses import replace
from itertools import chain, product
from pathlib import Path

import pytest

from pyjelly.options import LookupPreset
from tests.e2e_tests.ser_des.base_ser_des import BaseSerDes
from tests.e2e_tests.ser_des.rdflib_ser_des import RdflibSerDes


class End2EndOptionSetup:
    """Set up stream options, file size and file name for E2E tests."""

    test_root: Path = Path("tests/e2e_test_cases/")

    def setup_ser_des(self) -> list[tuple[BaseSerDes, BaseSerDes, LookupPreset, int]]:
        """Set up test serializer, deserializer, options and frame_size."""
        ser = [RdflibSerDes()]
        des = [RdflibSerDes()]
        # We want to have a variety of options to test
        # Particularly examples of small lookup sizes
        # and a lack of prefix
        small = LookupPreset.small()
        no_prefixes = replace(LookupPreset.small(), max_prefixes=0)
        tiny_lookups = replace(LookupPreset.small(), max_names=16, max_prefixes=8)
        big = LookupPreset()
        presets = [small, no_prefixes, tiny_lookups, big]
        frame_sizes = [1, 4, 200, 10_000]
        return list(product(ser, des, presets, frame_sizes))

    def setup_triple_files(
        self,
    ) -> list[tuple[BaseSerDes, BaseSerDes, LookupPreset, int, Path]]:
        """Set up options for each of the test triple files."""
        test_dir: Path = self.test_root / "triples_rdf_1_1"
        files = test_dir.glob("*.nt")
        options = self.setup_ser_des()
        return list(chain(*[[(*o, f) for o in options] for f in files]))

    def setup_quad_files(
        self,
    ) -> list[tuple[BaseSerDes, BaseSerDes, LookupPreset, int, Path]]:
        """Set up options for each of the test quad files."""
        test_dir: Path = self.test_root / "quads_rdf_1_1"
        files = test_dir.glob("*.nq")
        options = self.setup_ser_des()
        return list(chain(*[[(*o, f) for o in options] for f in files]))


class TestEnd2End:
    setup = End2EndOptionSetup()

    @pytest.mark.parametrize(
        ("ser", "des", "preset", "frame_size", "file"), setup.setup_triple_files()
    )
    def test_triple_files(
        self,
        ser: BaseSerDes,
        des: BaseSerDes,
        preset: LookupPreset,
        frame_size: int,
        file: Path,
    ) -> None:
        nt_reader = RdflibSerDes()
        with file.open("rb") as f:
            triples = nt_reader.read_triples(f.read())
            jelly_io = ser.write_triples_jelly(triples, preset, frame_size)
            new_g = des.read_triples_jelly(jelly_io)
            assert set(triples) == set(new_g)

    @pytest.mark.parametrize(
        ("ser", "des", "preset", "frame_size", "file"), setup.setup_quad_files()
    )
    def test_quad_files(
        self,
        ser: BaseSerDes,
        des: BaseSerDes,
        preset: LookupPreset,
        frame_size: int,
        file: Path,
    ) -> None:
        nq_reader = RdflibSerDes()
        with file.open("rb") as f:
            quads = nq_reader.read_quads(f.read())
            jelly_io = ser.write_quads_jelly(quads, preset, frame_size)
            new_g = des.read_quads_jelly(jelly_io)
            assert set(quads) == set(new_g)
