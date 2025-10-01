from tests.utils.rdf_test_cases import jelly_cli, needs_jelly_cli
from functools import partial

jelly_to_jelly = partial(jelly_cli, "rdf", "to-jelly")
