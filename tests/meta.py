from pathlib import Path

TESTS_DIR = Path(__file__).parent
CONFORMANCE_TESTS_DIR = TESTS_DIR / "conformance_tests"
E2E_TEST_CASES_DIR = TESTS_DIR / "e2e_test_cases"

TEST_OUTPUTS_DIR = TESTS_DIR / "out"

RDF_TESTS_DIR = CONFORMANCE_TESTS_DIR / "rdf"
RDF_FROM_JELLY_TESTS_DIR = RDF_TESTS_DIR / "from_jelly"
RDF_TO_JELLY_TESTS_DIR = RDF_TESTS_DIR / "to_jelly"
