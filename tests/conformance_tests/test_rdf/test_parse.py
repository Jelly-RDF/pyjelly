from __future__ import annotations

import urllib.parse
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph, Literal, Namespace, Node, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import RDF
from rdflib.plugins.serializers.nt import _quoteLiteral

from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
from pyjelly.integrations.rdflib.parse import parse_jelly_grouped
from tests.meta import TEST_OUTPUTS_DIR
from tests.utils.generic_sink_test_serializer import GenericSinkSerializer
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
RDFT = Namespace("http://www.w3.org/ns/rdftest#")


def _new_nq_row(triple: tuple[Node, Node, Node], context: Graph) -> str:
    template = "%s " * (3 + (context != DATASET_DEFAULT_GRAPH_ID)) + ".\n"
    args = (
        triple[0].n3(),
        triple[1].n3(),
        _quoteLiteral(triple[2]) if isinstance(triple[2], Literal) else triple[2].n3(),
        *((context.n3(),) if context != DATASET_DEFAULT_GRAPH_ID else ()),
    )
    return template % args


workaround_rdflib_serializes_default_graph_id = patch(
    "rdflib.plugins.serializers.nquads._nq_row",
    new=_new_nq_row,
)
workaround_rdflib_serializes_default_graph_id.start()


class ManifestParser:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = manifest_path
        self.graph = Graph()
        with manifest_path.open("rb") as f:
            self.graph.parse(f, format="turtle", publicID=str(manifest_path))
        self.manifest_uri = URIRef(manifest_path.as_uri())

    def get_test_cases(self, test_type: str | None = None) -> list[dict[str, Any]]:
        test_cases: list[dict[str, Any]] = []
        entries = self.graph.objects(self.manifest_uri, MF.entries)
        for entry_list in entries:
            for test_uri in self.graph.items(entry_list):
                if not isinstance(test_uri, URIRef):
                    continue
                test_case = self._parse_test_case(test_uri)
                if test_type is None or test_case.get("type") == test_type:
                    test_cases.append(test_case)
        return test_cases

    def _parse_test_case(self, test_uri: URIRef) -> dict[str, Any]:
        test_case: dict[str, Any] = {"uri": str(test_uri)}
        if (test_uri, RDF.type, JELLYT.TestPositive) in self.graph:
            test_case["type"] = "positive"
        elif (test_uri, RDF.type, JELLYT.TestNegative) in self.graph:
            test_case["type"] = "negative"

        name = self.graph.value(test_uri, MF.name)
        if name:
            test_case["name"] = str(name)

        action = self.graph.value(test_uri, MF.action)
        if action:
            if isinstance(action, URIRef):
                test_case["action"] = [action]
            else:
                test_case["action"] = list(self.graph.items(action))

        result = self.graph.value(test_uri, MF.result)
        if result:
            if isinstance(result, URIRef):
                test_case["result"] = [result]
            else:
                test_case["result"] = list(self.graph.items(result))

        test_case["requirements"] = [
            str(req) for req in self.graph.objects(test_uri, MF.requires)
        ]

        uri_str = str(test_uri)
        if "generalized" in uri_str:
            test_case["category"] = "generalized"
        elif "rdf_star" in uri_str:
            test_case["category"] = "rdf_star"
        else:
            test_case["category"] = "rdf_1_1"

        return test_case


def uri_to_local_path(uri: URIRef, base_dir: Path) -> Path:
    uri_str = str(uri)
    if uri_str.startswith("file://"):
        path_str = urllib.parse.unquote(uri_str[7:])
        path_str = path_str.removeprefix("/")
        return Path(path_str)
    if "://" not in uri_str:
        return base_dir / uri_str
    parsed_url = urllib.parse.urlparse(uri_str)
    path_component = parsed_url.path
    path_component = path_component.removeprefix("/")
    return base_dir / path_component


PROTOBUF_SUBMODULE_DIR = (
    Path(__file__).parent.parent.parent.parent / "submodules" / "protobuf"
)
FROM_JELLY_MANIFEST = (
    PROTOBUF_SUBMODULE_DIR / "test" / "rdf" / "from_jelly" / "manifest.ttl"
)
TO_JELLY_MANIFEST = (
    PROTOBUF_SUBMODULE_DIR / "test" / "rdf" / "to_jelly" / "manifest.ttl"
)

if not FROM_JELLY_MANIFEST.exists():
    _msg_from = f"From Jelly manifest not found: {FROM_JELLY_MANIFEST}"
    raise FileNotFoundError(_msg_from)
if not TO_JELLY_MANIFEST.exists():
    _msg_to = f"To Jelly manifest not found: {TO_JELLY_MANIFEST}"
    raise FileNotFoundError(_msg_to)

from_manifest = ManifestParser(FROM_JELLY_MANIFEST)
to_manifest = ManifestParser(TO_JELLY_MANIFEST)


def run_from_jelly_positive_test(
    test_case: dict[str, Any],
    test_id: str,
    *,
    use_generic: bool = False,
) -> None:
    """Run positive test case — should parse successfully."""
    if not test_case.get("action"):
        pytest.skip(f"No action defined for test {test_id}")

    input_uri = test_case["action"][0]
    input_path = uri_to_local_path(
        input_uri, PROTOBUF_SUBMODULE_DIR / "test" / "rdf" / "from_jelly"
    )

    if not input_path.exists():
        pytest.skip(f"Input file not found: {input_path}")

    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True, parents=True)

    if use_generic:
        with input_path.open("rb") as input_file:
            for frame_no, sink in enumerate(generic_parse_jelly_grouped(input_file)):
                extension = "nt" if "triples" in test_id else "nq"
                output_filename = output_dir / f"out_{frame_no:03}.{extension}"
                serializer = GenericSinkSerializer(sink)
                serializer.serialize(output_filename=output_filename, encoding="utf-8")
                jelly_validate(
                    input_path,
                    "--compare-ordered",
                    "--compare-frame-indices",
                    frame_no,
                    "--compare-to-rdf-file",
                    output_filename,
                    hint=f"Test ID: {test_id}, output file: {output_filename}",
                )
    else:
        with input_path.open("rb") as input_file:
            for frame_no, graph in enumerate(
                parse_jelly_grouped(
                    input_file,
                    graph_factory=lambda: Graph(store=OrderedMemory()),
                    dataset_factory=lambda: Dataset(store=OrderedMemory()),
                )
            ):
                extension = f"n{'quads' if isinstance(graph, Dataset) else 'triples'}"
                output_filename = output_dir / f"out_{frame_no:03}.{extension[:2]}"
                graph.serialize(
                    destination=output_filename, encoding="utf-8", format=extension
                )
                jelly_validate(
                    input_path,
                    "--compare-ordered",
                    "--compare-frame-indices",
                    frame_no,
                    "--compare-to-rdf-file",
                    output_filename,
                    hint=f"Test ID: {test_id}, output file: {output_filename}",
                )


def run_from_jelly_negative_test(
    test_case: dict[str, Any],
    test_id: str,
    *,
    use_generic: bool = False,
) -> None:
    """Run negative test case — should fail to parse."""
    if not test_case.get("action"):
        pytest.skip(f"No action defined for test {test_id}")

    input_uri = test_case["action"][0]
    input_path = uri_to_local_path(
        input_uri, PROTOBUF_SUBMODULE_DIR / "test" / "rdf" / "from_jelly"
    )

    if not input_path.exists():
        pytest.skip(f"Input file not found: {input_path}")

    if use_generic:
        with (
            pytest.raises(BaseException, match=".") as _excinfo,
            input_path.open("rb") as input_file,
        ):
            list(generic_parse_jelly_grouped(input_file))
    else:
        dataset = Dataset(store=OrderedMemory())
        with pytest.raises(BaseException, match=".") as _excinfo:
            dataset.parse(location=str(input_path), format="jelly")


def run_to_jelly_test(_test_case: dict[str, Any], _test_id: str) -> None:
    pytest.skip("to_jelly tests not implemented in this example")


for test_case in from_manifest.get_test_cases("positive"):
    test_id = test_case["uri"].split("/")[-1]

    @needs_jelly_cli
    def test_from_jelly_positive_rdflib(
        test_case: dict[str, Any] = test_case, test_id: str = test_id
    ) -> None:
        run_from_jelly_positive_test(test_case, test_id, use_generic=False)

    test_from_jelly_positive_rdflib.__name__ = (
        f"test_from_jelly_positive_rdflib_{test_id}"
    )
    globals()[test_from_jelly_positive_rdflib.__name__] = (
        test_from_jelly_positive_rdflib
    )

for test_case in from_manifest.get_test_cases("positive"):
    test_id = test_case["uri"].split("/")[-1]

    if test_case.get("category") in ["generalized", "rdf_star"]:

        @needs_jelly_cli
        def test_from_jelly_positive_generic(
            test_case: dict[str, Any] = test_case, test_id: str = test_id
        ) -> None:
            run_from_jelly_positive_test(test_case, test_id, use_generic=True)

        test_from_jelly_positive_generic.__name__ = (
            f"test_from_jelly_positive_generic_{test_id}"
        )
        globals()[test_from_jelly_positive_generic.__name__] = (
            test_from_jelly_positive_generic
        )

for test_case in from_manifest.get_test_cases("negative"):
    test_id = test_case["uri"].split("/")[-1]

    @needs_jelly_cli
    def test_from_jelly_negative_rdflib(
        test_case: dict[str, Any] = test_case, test_id: str = test_id
    ) -> None:
        run_from_jelly_negative_test(test_case, test_id, use_generic=False)

    test_from_jelly_negative_rdflib.__name__ = (
        f"test_from_jelly_negative_rdflib_{test_id}"
    )
    globals()[test_from_jelly_negative_rdflib.__name__] = (
        test_from_jelly_negative_rdflib
    )

for test_case in from_manifest.get_test_cases("negative"):
    test_id = test_case["uri"].split("/")[-1]

    @needs_jelly_cli
    def test_from_jelly_negative_generic(
        test_case: dict[str, Any] = test_case, test_id: str = test_id
    ) -> None:
        run_from_jelly_negative_test(test_case, test_id, use_generic=True)

    test_from_jelly_negative_generic.__name__ = (
        f"test_from_jelly_negative_generic_{test_id}"
    )
    globals()[test_from_jelly_negative_generic.__name__] = (
        test_from_jelly_negative_generic
    )

for test_case in to_manifest.get_test_cases():
    test_id = test_case["uri"].split("/")[-1]

    @needs_jelly_cli
    def test_to_jelly_placeholder(
        test_case: dict[str, Any] = test_case,
        test_id: str = test_id,
    ) -> None:
        run_to_jelly_test(test_case, test_id)

    test_to_jelly_placeholder.__name__ = f"test_to_jelly_placeholder_{test_id}"
    globals()[test_to_jelly_placeholder.__name__] = test_to_jelly_placeholder
