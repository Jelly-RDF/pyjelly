from pyjelly.jelly import rdf_pb2, sparql_pb2
from pyjelly.jelly.rdf_pb2 import *
from pyjelly.jelly.sparql_pb2 import *

# workaround for mypyc
globals().update(vars(rdf_pb2))
globals().update(vars(sparql_pb2))
