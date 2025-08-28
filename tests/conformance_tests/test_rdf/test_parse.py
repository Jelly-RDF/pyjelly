from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph, Namespace, Node, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import RDF

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


@dataclass
class JellyTestCase:
    uri: str
    type: str | None = None
    name: str | None = None
    action: list[URIRef] = field(default_factory=list)
    result: list[Node] = field(default_factory=list)
    requirements: list[str] = field(default_factory=list)
    category: str | None = None


@dataclass
class ManifestParser:
    manifest_path: Path
    graph: Graph = field(init=False)
    manifest_uri: URIRef = field(init=False)

    def __post_init__(self) -> None:
        self.graph = Graph()
        with self.manifest_path.open("rb") as f:
            self.graph.parse(f, format="turtle", publicID=str(self.manifest_path))
        self.manifest_uri = URIRef(self.manifest_path.as_uri())

    def get_test_cases(self, test_type: str | None = None) -> list[JellyTestCase]:
        test_cases: list[JellyTestCase] = []
        entries = self.graph.objects(self.manifest_uri, MF.entries)
        for entry_list in entries:
            for test_uri in self.graph.items(entry_list):
                if not isinstance(test_uri, URIRef):
                    continue
                test_case = self._parse_test_case(test_uri)
                if test_type is None or test_case.type == test_type:
                    test_cases.append(test_case)
        return test_cases

    def _parse_test_case(self, test_uri: URIRef) -> JellyTestCase:
        test_type = None
        if (test_uri, RDF.type, JELLYT.TestPositive) in self.graph:
            test_type = "positive"
        elif (test_uri, RDF.type, JELLYT.TestNegative) in self.graph:
            test_type = "negative"
        name = self.graph.value(test_uri, MF.name)
        name_str = str(name) if name else None
        action = self.graph.value(test_uri, MF.action)
        action_list: list[URIRef] = []
        if action:
            if isinstance(action, URIRef):
                action_list = [action]
            else:
                action_list = [
                    x for x in self.graph.items(action) if isinstance(x, URIRef)
                ]
        result = self.graph.value(test_uri, MF.result)
        result_list: list[Node] = []
        if result:
            if isinstance(result, URIRef):
                result_list = [result]
            else:
                result_list = list(self.graph.items(result))
        requirements = [str(req) for req in self.graph.objects(test_uri, MF.requires)]
        uri_str = str(test_uri)
        if "generalized" in uri_str:
            category = "generalized"
        elif "rdf_star" in uri_str:
            category = "rdf_star"
        else:
            category = "rdf_1_1"
        return JellyTestCase(
            uri=str(test_uri),
            type=test_type,
            name=name_str,
            action=action_list,
            result=result_list,
            requirements=requirements,
            category=category,
        )


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


def _new_nq_row(triple: tuple[Node, Node, Node], context: Graph) -> str:
    s, p, o = triple
    context_str = ""
    if context.identifier != DATASET_DEFAULT_GRAPH_ID:
        context_str = f" {context.identifier.n3()}"
    return f"{s.n3()} {p.n3()} {o.n3()}{context_str} .\n"


workaround_rdflib_serializes_default_graph_id = patch(
    "rdflib.plugins.serializers.nquads._nq_row",
    new=_new_nq_row,
)
workaround_rdflib_serializes_default_graph_id.start()

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
    msg = f"Missing manifest: {FROM_JELLY_MANIFEST}"
    raise FileNotFoundError(msg)
if not TO_JELLY_MANIFEST.exists():
    msg = f"Missing manifest: {TO_JELLY_MANIFEST}"
    raise FileNotFoundError(msg)

from_manifest = ManifestParser(FROM_JELLY_MANIFEST)
to_manifest = ManifestParser(TO_JELLY_MANIFEST)


def run_from_jelly_positive_test(
    test_case: JellyTestCase,
    test_id: str,
    *,
    use_generic: bool = False,
) -> None:
    if not test_case.action:
        pytest.skip(f"No action defined for test {test_id}")
    input_uri = test_case.action[0]
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
    test_case: JellyTestCase,
    test_id: str,
    *,
    use_generic: bool = False,
) -> None:
    if not test_case.action:
        pytest.skip(f"No action defined for test {test_id}")
    input_uri = test_case.action[0]
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


def run_to_jelly_test(_test_case: JellyTestCase, _test_id: str) -> None:
    pytest.skip("to_jelly tests not implemented in this example")


for test_case in from_manifest.get_test_cases("positive"):
    test_id = test_case.uri.split("/")[-1]

    @needs_jelly_cli
    def test_from_jelly_positive_rdflib(
        test_case: JellyTestCase = test_case, test_id: str = test_id
    ) -> None:
        run_from_jelly_positive_test(test_case, test_id, use_generic=False)

    test_from_jelly_positive_rdflib.__name__ = (
        f"test_from_jelly_positive_rdflib_{test_id}"
    )
    name_pos = test_from_jelly_positive_rdflib.__name__
    globals()[name_pos] = test_from_jelly_positive_rdflib

for test_case in from_manifest.get_test_cases("positive"):
    test_id = test_case.uri.split("/")[-1]
    if test_case.category in ["generalized", "rdf_star"]:

        @needs_jelly_cli
        def test_from_jelly_positive_generic(
            test_case: JellyTestCase = test_case, test_id: str = test_id
        ) -> None:
            run_from_jelly_positive_test(test_case, test_id, use_generic=True)

        test_from_jelly_positive_generic.__name__ = (
            f"test_from_jelly_positive_generic_{test_id}"
        )
        name_pos_gen = test_from_jelly_positive_generic.__name__
        globals()[name_pos_gen] = test_from_jelly_positive_generic

for test_case in from_manifest.get_test_cases("negative"):
    test_id = test_case.uri.split("/")[-1]

    @needs_jelly_cli
    def test_from_jelly_negative_rdflib(
        test_case: JellyTestCase = test_case, test_id: str = test_id
    ) -> None:
        run_from_jelly_negative_test(test_case, test_id, use_generic=False)

    test_from_jelly_negative_rdflib.__name__ = (
        f"test_from_jelly_negative_rdflib_{test_id}"
    )
    name_neg = test_from_jelly_negative_rdflib.__name__
    globals()[name_neg] = test_from_jelly_negative_rdflib

for test_case in from_manifest.get_test_cases("negative"):
    test_id = test_case.uri.split("/")[-1]

    @needs_jelly_cli
    def test_from_jelly_negative_generic(
        test_case: JellyTestCase = test_case, test_id: str = test_id
    ) -> None:
        run_from_jelly_negative_test(test_case, test_id, use_generic=True)

    test_from_jelly_negative_generic.__name__ = (
        f"test_from_jelly_negative_generic_{test_id}"
    )
    name_neg_gen = test_from_jelly_negative_generic.__name__
    globals()[name_neg_gen] = test_from_jelly_negative_generic

for test_case in to_manifest.get_test_cases():
    test_id = test_case.uri.split("/")[-1]

    @needs_jelly_cli
    def test_to_jelly_placeholder(
        test_case: JellyTestCase = test_case,
        test_id: str = test_id,
    ) -> None:
        run_to_jelly_test(test_case, test_id)

    test_to_jelly_placeholder.__name__ = f"test_to_jelly_placeholder_{test_id}"
    name_to = test_to_jelly_placeholder.__name__
    globals()[name_to] = test_to_jelly_placeholder
