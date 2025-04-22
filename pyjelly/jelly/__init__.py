from pyjelly.jelly import rdf_pb2
from pyjelly.jelly.rdf_pb2 import *

# workaround for mypyc
globals().update(vars(rdf_pb2))
