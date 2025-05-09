import io
import logging
import pytest
import subtests
from pathlib import Path
from rdflib import Graph

from pyjelly.options import StreamOptions
from tests.utils.ordered_memory import OrderedMemory

logger = logging.getLogger(__name__)

def parse_rdf_lib(input_path: Path = "pyjelly/tests/e2e_tests/test_cases/triples_rdf_1_1/weather.nt", format: str = "nt") -> None:
    g = Graph(store=OrderedMemory())
    g.parse(input_path, format=format)
    byte_stream = io.BytesIO()
    g.serialize(destination=byte_stream, format="jelly")
    new_g = Graph(store=OrderedMemory())
    new_g.parse(data=byte_stream.getvalue(), format="jelly")
    return g, new_g
    
def test_triple_files(test_dir: str = "pyjelly/tests/e2e_tests/test_cases/triples_rdf_1_1/") -> None:
    for file in Path(test_dir).glob("*.nt"):
        with subtests.test(msg=f"Testing {file} with format nt"):
            g, new_g = parse_rdf_lib(file, format="nt")
            assert new_g.serialize(format="nt") == g.serialize(format="nt")

def test_quad_files(test_dir: str = "pyjelly/tests/e2e_tests/test_cases/quads_rdf_1_1/") -> None:
    for file in Path(test_dir).glob("*.nq"):
        with subtests.test(msg=f"Testing {file} with format nq"):
            g, new_g = parse_rdf_lib(file, format="nq")
            assert new_g.serialize(format="nq") == g.serialize(format="nq")


'''  
@pytest.mark.parametrize(
    "stream_opts",
    [
        StreamOptions(
            encoding="utf-8",
            pretty_print=True,
            indent=2,
            base_iri="http://example.org/",
        ),
        StreamOptions(
            encoding="utf-8",
            pretty_print=False,
            indent=0,
            base_iri="http://example.org/",
        ),
    ]
)  
'''      
def run_test() -> None:
    test_triple_files()
    test_quad_files()
    
if __name__ == "__main__":
    run_test()