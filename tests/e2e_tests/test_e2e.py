from __future__ import annotations

import tempfile
from dataclasses import replace
from functools import partial
from itertools import chain, product
from pathlib import Path

import pytest

from pyjelly.options import LookupPreset
from tests.e2e_tests.ser_des.base_ser_des import BaseSerDes
from tests.e2e_tests.ser_des.generic_ser_des import GenericSerDes
from tests.e2e_tests.ser_des.rdflib_ser_des import RdflibSerDes
from tests.utils.rdf_test_cases import jelly_cli, needs_jelly_cli

DEFAULT_PRESET = replace(LookupPreset.small(), max_prefixes=0)
DEFAULT_FRAME_SIZE = 200

jelly_validate = partial(jelly_cli, "rdf", "validate", "--compare-ordered")
jelly_from_jelly = partial(jelly_cli, "rdf", "from-jelly")
rdf_to_jelly = partial(jelly_cli, "rdf", "to-jelly")


class End2EndOptionSetup:
    """Set up stream options, file size and file name for rdf E2E tests."""

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


class End2EndOptionSetupGeneric:
    """Set up stream options, file size and file name for generic sink E2E tests."""

    test_root: Path = Path("tests/e2e_test_cases/")

    def setup_ser_des(
        self,
    ) -> list[tuple[GenericSerDes, GenericSerDes, LookupPreset, int]]:
        ser = [GenericSerDes()]
        des = [GenericSerDes()]
        small = LookupPreset.small()
        no_prefixes = replace(LookupPreset.small(), max_prefixes=0)
        tiny_lookups = replace(LookupPreset.small(), max_names=16, max_prefixes=8)
        big = LookupPreset()
        presets = [small, no_prefixes, tiny_lookups, big]
        frame_sizes = [1, 4, 200, 10_000]
        return list(product(ser, des, presets, frame_sizes))

    def setup_triple_files(
        self,
    ) -> list[tuple[GenericSerDes, GenericSerDes, LookupPreset, int, Path]]:
        test_dir: Path = self.test_root / "triples_rdf_1_1"
        files = test_dir.glob("*.nt")
        options = self.setup_ser_des()
        return list(chain(*[[(*o, f) for o in options] for f in files]))

    def setup_quad_files(
        self,
    ) -> list[tuple[GenericSerDes, GenericSerDes, LookupPreset, int, Path]]:
        test_dir: Path = self.test_root / "quads_rdf_1_1"
        files = test_dir.glob("*.nq")
        options = self.setup_ser_des()
        return list(chain(*[[(*o, f) for o in options] for f in files]))


class End2EndOptionSetupCross:
    """Set up stream options, file size and file name for cross E2E tests."""

    rdf_root: Path = Path("tests/e2e_test_cases/")

    def triples(self) -> list[Path]:
        return sorted((self.rdf_root / "triples_rdf_1_1").glob("*.nt"))

    def quads(self) -> list[Path]:
        return sorted((self.rdf_root / "quads_rdf_1_1").glob("*.nq"))


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


class TestEnd2EndGeneric:
    setup = End2EndOptionSetupGeneric()

    @needs_jelly_cli
    @pytest.mark.parametrize(
        ("ser", "des", "preset", "frame_size", "file"), setup.setup_triple_files()
    )
    def test_triple_files(
        self,
        ser: GenericSerDes,
        des: GenericSerDes,
        preset: LookupPreset,
        frame_size: int,
        file: Path,
    ) -> None:
        reader = GenericSerDes()
        gen_input_jelly = rdf_to_jelly(file)
        triples = reader.read_triples(gen_input_jelly)
        jelly = ser.write_triples_jelly(triples, preset, frame_size)
        new_g = des.read_triples(jelly)
        assert set(triples) == set(new_g)

    @needs_jelly_cli
    @pytest.mark.parametrize(
        ("ser", "des", "preset", "frame_size", "file"), setup.setup_quad_files()
    )
    def test_quad_files(
        self,
        ser: GenericSerDes,
        des: GenericSerDes,
        preset: LookupPreset,
        frame_size: int,
        file: Path,
    ) -> None:
        reader = GenericSerDes()
        gen_input_jelly = rdf_to_jelly(file)
        quads = reader.read_quads(gen_input_jelly)
        jelly = ser.write_quads_jelly(quads, preset, frame_size)
        new_g = des.read_quads(jelly)
        assert set(quads) == set(new_g)


class TestEnd2EndCross:
    setup = End2EndOptionSetupCross()
    triples_cases = setup.triples()
    quads_cases = setup.quads()

    @needs_jelly_cli
    @pytest.mark.parametrize(
        "r_path",
        triples_cases,
        ids=[f"triples:{p.name}" for p in triples_cases],
    )
    def test_cross_generic_rdf_triple(self, r_path: Path) -> None:
        gen = GenericSerDes()
        rdf = RdflibSerDes()
        gen_input_jelly = rdf_to_jelly(r_path)
        g_triples = gen.read_triples(gen_input_jelly)
        g_jelly = gen.write_triples(g_triples)
        with r_path.open("rb") as file_rdf:
            r_triples = rdf.read_triples(file_rdf.read())
            r_jelly = rdf.write_triples_jelly(
                r_triples, DEFAULT_PRESET, DEFAULT_FRAME_SIZE
            )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_gen = Path(temp_dir) / "gen.jelly"
            temp_rdf = Path(temp_dir) / "rdf.jelly"
            temp_gen.write_bytes(g_jelly)
            temp_rdf.write_bytes(r_jelly)
            jelly_validate(temp_gen, "--compare-to-rdf-file", temp_rdf)

    @needs_jelly_cli
    @pytest.mark.parametrize(
        "r_path",
        quads_cases,
        ids=[f"quads:{p.name}" for p in quads_cases],
    )
    def test_cross_generic_rdf_quad(self, r_path: Path) -> None:
        gen = GenericSerDes()
        rdf = RdflibSerDes()
        gen_input_jelly = rdf_to_jelly(r_path)
        g_quads = gen.read_quads(gen_input_jelly)
        g_jelly = gen.write_quads(g_quads)
        with r_path.open("rb") as file_rdf:
            r_quads = rdf.read_quads(file_rdf.read())
            r_jelly = rdf.write_quads_jelly(r_quads, DEFAULT_PRESET, DEFAULT_FRAME_SIZE)
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_gen = Path(temp_dir) / "gen.jelly"
            temp_rdf = Path(temp_dir) / "rdf.jelly"
            temp_gen.write_bytes(g_jelly)
            temp_rdf.write_bytes(r_jelly)
            jelly_validate(temp_gen, "--compare-to-rdf-file", temp_rdf)
