import rdf_pb2
from rdflib import URIRef, BNode, Literal
from rdflib import Graph, Namespace

prefix_lookup = {}
name_lookup = {}
datatype_lookup = {}


filename = "./output/encoded_file.bin"
rdf_stream_frame = rdf_pb2.RdfStreamFrame()
with open(filename, "rb") as f:
  rdf_stream_frame.ParseFromString(f.read())
  
g = Graph()
options_row = rdf_stream_frame.rows.pop(0)
for m in rdf_stream_frame.rows:
    if m.HasField("prefix"):
        prefix_lookup.update({m.prefix.id: m.prefix.value})
    elif m.HasField("name"):
        if m.name.value != '':
            name_lookup.update({m.name.id: m.name.value})
    elif m.HasField("namespace"):
        g.namespace_manager.bind(m.namespace.name, Namespace(prefix_lookup[m.namespace.value.prefix_id]))
    elif m.HasField("datatype"):
        datatype_lookup.update({m.datatype.id: m.datatype.value})
    elif m.HasField("triple"):
        if m.triple.s_iri.prefix_id > 0:
            sbj = URIRef(prefix_lookup[m.triple.s_iri.prefix_id]+name_lookup[m.triple.s_iri.name_id])
        if m.triple.s_bnode != '':
            sbj = BNode(value = m.triple.s_bnode)
        if m.triple.s_literal.lex != '':
            if m.triple.s_literal.langtag != '':
                obj = Literal(m.triple.s_literal.lex, lang=m.triple.s_literal.langtag)
            elif m.triple.s_literal.datatype > 0:
                obj = Literal(m.triple.s_literal.lex, datatype=datatype_lookup[m.triple.s_literal.datatype])  
            else:
                obj = Literal(m.triple.s_literal.lex)  
        
        if m.triple.p_iri.prefix_id > 0:
            pred = URIRef(prefix_lookup[m.triple.p_iri.prefix_id]+name_lookup[m.triple.p_iri.name_id])
        if m.triple.p_bnode != '':
            pred = BNode(value = m.triple.p_bnode)
        if m.triple.p_literal.lex != '':
            if m.triple.p_literal.langtag != '':
                obj = Literal(m.triple.p_literal.lex, lang=m.triple.p_literal.langtag)
            elif m.triple.p_literal.datatype > 0:
                obj = Literal(m.triple.p_literal.lex, datatype=datatype_lookup[m.triple.p_literal.datatype])  
            else:
                obj = Literal(m.triple.p_literal.lex)  

        if m.triple.o_iri.prefix_id > 0:
            obj = URIRef(prefix_lookup[m.triple.o_iri.prefix_id]+name_lookup[m.triple.o_iri.name_id])
        if m.triple.o_bnode != '':
            obj = BNode(value = m.triple.o_bnode)
        if m.triple.o_literal.lex != '':
            if m.triple.o_literal.langtag != '':
                obj = Literal(m.triple.o_literal.lex, lang=m.triple.o_literal.langtag)
            elif m.triple.o_literal.datatype > 0:
                obj = Literal(m.triple.o_literal.lex, datatype=datatype_lookup[m.triple.o_literal.datatype])  
            else:
                obj = Literal(m.triple.o_literal.lex)
        
        g.add((sbj, pred, obj))
    else:
        print("new message")
        
g.serialize("./output/output.ttl", format="ttl")