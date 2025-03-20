from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PhysicalStreamType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PHYSICAL_STREAM_TYPE_UNSPECIFIED: _ClassVar[PhysicalStreamType]
    PHYSICAL_STREAM_TYPE_TRIPLES: _ClassVar[PhysicalStreamType]
    PHYSICAL_STREAM_TYPE_QUADS: _ClassVar[PhysicalStreamType]
    PHYSICAL_STREAM_TYPE_GRAPHS: _ClassVar[PhysicalStreamType]

class LogicalStreamType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOGICAL_STREAM_TYPE_UNSPECIFIED: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_FLAT_TRIPLES: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_FLAT_QUADS: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_GRAPHS: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_DATASETS: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_NAMED_GRAPHS: _ClassVar[LogicalStreamType]
    LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS: _ClassVar[LogicalStreamType]
PHYSICAL_STREAM_TYPE_UNSPECIFIED: PhysicalStreamType
PHYSICAL_STREAM_TYPE_TRIPLES: PhysicalStreamType
PHYSICAL_STREAM_TYPE_QUADS: PhysicalStreamType
PHYSICAL_STREAM_TYPE_GRAPHS: PhysicalStreamType
LOGICAL_STREAM_TYPE_UNSPECIFIED: LogicalStreamType
LOGICAL_STREAM_TYPE_FLAT_TRIPLES: LogicalStreamType
LOGICAL_STREAM_TYPE_FLAT_QUADS: LogicalStreamType
LOGICAL_STREAM_TYPE_GRAPHS: LogicalStreamType
LOGICAL_STREAM_TYPE_DATASETS: LogicalStreamType
LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS: LogicalStreamType
LOGICAL_STREAM_TYPE_NAMED_GRAPHS: LogicalStreamType
LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS: LogicalStreamType

class RdfIri(_message.Message):
    __slots__ = ("prefix_id", "name_id")
    PREFIX_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_ID_FIELD_NUMBER: _ClassVar[int]
    prefix_id: int
    name_id: int
    def __init__(self, prefix_id: _Optional[int] = ..., name_id: _Optional[int] = ...) -> None: ...

class RdfLiteral(_message.Message):
    __slots__ = ("lex", "langtag", "datatype")
    LEX_FIELD_NUMBER: _ClassVar[int]
    LANGTAG_FIELD_NUMBER: _ClassVar[int]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    lex: str
    langtag: str
    datatype: int
    def __init__(self, lex: _Optional[str] = ..., langtag: _Optional[str] = ..., datatype: _Optional[int] = ...) -> None: ...

class RdfDefaultGraph(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RdfTriple(_message.Message):
    __slots__ = ("s_iri", "s_bnode", "s_literal", "s_triple_term", "p_iri", "p_bnode", "p_literal", "p_triple_term", "o_iri", "o_bnode", "o_literal", "o_triple_term")
    S_IRI_FIELD_NUMBER: _ClassVar[int]
    S_BNODE_FIELD_NUMBER: _ClassVar[int]
    S_LITERAL_FIELD_NUMBER: _ClassVar[int]
    S_TRIPLE_TERM_FIELD_NUMBER: _ClassVar[int]
    P_IRI_FIELD_NUMBER: _ClassVar[int]
    P_BNODE_FIELD_NUMBER: _ClassVar[int]
    P_LITERAL_FIELD_NUMBER: _ClassVar[int]
    P_TRIPLE_TERM_FIELD_NUMBER: _ClassVar[int]
    O_IRI_FIELD_NUMBER: _ClassVar[int]
    O_BNODE_FIELD_NUMBER: _ClassVar[int]
    O_LITERAL_FIELD_NUMBER: _ClassVar[int]
    O_TRIPLE_TERM_FIELD_NUMBER: _ClassVar[int]
    s_iri: RdfIri
    s_bnode: str
    s_literal: RdfLiteral
    s_triple_term: RdfTriple
    p_iri: RdfIri
    p_bnode: str
    p_literal: RdfLiteral
    p_triple_term: RdfTriple
    o_iri: RdfIri
    o_bnode: str
    o_literal: RdfLiteral
    o_triple_term: RdfTriple
    def __init__(self, s_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., s_bnode: _Optional[str] = ..., s_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ..., s_triple_term: _Optional[_Union[RdfTriple, _Mapping]] = ..., p_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., p_bnode: _Optional[str] = ..., p_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ..., p_triple_term: _Optional[_Union[RdfTriple, _Mapping]] = ..., o_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., o_bnode: _Optional[str] = ..., o_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ..., o_triple_term: _Optional[_Union[RdfTriple, _Mapping]] = ...) -> None: ...

class RdfQuad(_message.Message):
    __slots__ = ("s_iri", "s_bnode", "s_literal", "s_triple_term", "p_iri", "p_bnode", "p_literal", "p_triple_term", "o_iri", "o_bnode", "o_literal", "o_triple_term", "g_iri", "g_bnode", "g_default_graph", "g_literal")
    S_IRI_FIELD_NUMBER: _ClassVar[int]
    S_BNODE_FIELD_NUMBER: _ClassVar[int]
    S_LITERAL_FIELD_NUMBER: _ClassVar[int]
    S_TRIPLE_TERM_FIELD_NUMBER: _ClassVar[int]
    P_IRI_FIELD_NUMBER: _ClassVar[int]
    P_BNODE_FIELD_NUMBER: _ClassVar[int]
    P_LITERAL_FIELD_NUMBER: _ClassVar[int]
    P_TRIPLE_TERM_FIELD_NUMBER: _ClassVar[int]
    O_IRI_FIELD_NUMBER: _ClassVar[int]
    O_BNODE_FIELD_NUMBER: _ClassVar[int]
    O_LITERAL_FIELD_NUMBER: _ClassVar[int]
    O_TRIPLE_TERM_FIELD_NUMBER: _ClassVar[int]
    G_IRI_FIELD_NUMBER: _ClassVar[int]
    G_BNODE_FIELD_NUMBER: _ClassVar[int]
    G_DEFAULT_GRAPH_FIELD_NUMBER: _ClassVar[int]
    G_LITERAL_FIELD_NUMBER: _ClassVar[int]
    s_iri: RdfIri
    s_bnode: str
    s_literal: RdfLiteral
    s_triple_term: RdfTriple
    p_iri: RdfIri
    p_bnode: str
    p_literal: RdfLiteral
    p_triple_term: RdfTriple
    o_iri: RdfIri
    o_bnode: str
    o_literal: RdfLiteral
    o_triple_term: RdfTriple
    g_iri: RdfIri
    g_bnode: str
    g_default_graph: RdfDefaultGraph
    g_literal: RdfLiteral
    def __init__(self, s_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., s_bnode: _Optional[str] = ..., s_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ..., s_triple_term: _Optional[_Union[RdfTriple, _Mapping]] = ..., p_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., p_bnode: _Optional[str] = ..., p_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ..., p_triple_term: _Optional[_Union[RdfTriple, _Mapping]] = ..., o_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., o_bnode: _Optional[str] = ..., o_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ..., o_triple_term: _Optional[_Union[RdfTriple, _Mapping]] = ..., g_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., g_bnode: _Optional[str] = ..., g_default_graph: _Optional[_Union[RdfDefaultGraph, _Mapping]] = ..., g_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ...) -> None: ...

class RdfGraphStart(_message.Message):
    __slots__ = ("g_iri", "g_bnode", "g_default_graph", "g_literal")
    G_IRI_FIELD_NUMBER: _ClassVar[int]
    G_BNODE_FIELD_NUMBER: _ClassVar[int]
    G_DEFAULT_GRAPH_FIELD_NUMBER: _ClassVar[int]
    G_LITERAL_FIELD_NUMBER: _ClassVar[int]
    g_iri: RdfIri
    g_bnode: str
    g_default_graph: RdfDefaultGraph
    g_literal: RdfLiteral
    def __init__(self, g_iri: _Optional[_Union[RdfIri, _Mapping]] = ..., g_bnode: _Optional[str] = ..., g_default_graph: _Optional[_Union[RdfDefaultGraph, _Mapping]] = ..., g_literal: _Optional[_Union[RdfLiteral, _Mapping]] = ...) -> None: ...

class RdfGraphEnd(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RdfNamespaceDeclaration(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: RdfIri
    def __init__(self, name: _Optional[str] = ..., value: _Optional[_Union[RdfIri, _Mapping]] = ...) -> None: ...

class RdfNameEntry(_message.Message):
    __slots__ = ("id", "value")
    ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: int
    value: str
    def __init__(self, id: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class RdfPrefixEntry(_message.Message):
    __slots__ = ("id", "value")
    ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: int
    value: str
    def __init__(self, id: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class RdfDatatypeEntry(_message.Message):
    __slots__ = ("id", "value")
    ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: int
    value: str
    def __init__(self, id: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class RdfStreamOptions(_message.Message):
    __slots__ = ("stream_name", "physical_type", "generalized_statements", "rdf_star", "max_name_table_size", "max_prefix_table_size", "max_datatype_table_size", "logical_type", "version")
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    PHYSICAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    GENERALIZED_STATEMENTS_FIELD_NUMBER: _ClassVar[int]
    RDF_STAR_FIELD_NUMBER: _ClassVar[int]
    MAX_NAME_TABLE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_PREFIX_TABLE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_DATATYPE_TABLE_SIZE_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    stream_name: str
    physical_type: PhysicalStreamType
    generalized_statements: bool
    rdf_star: bool
    max_name_table_size: int
    max_prefix_table_size: int
    max_datatype_table_size: int
    logical_type: LogicalStreamType
    version: int
    def __init__(self, stream_name: _Optional[str] = ..., physical_type: _Optional[_Union[PhysicalStreamType, str]] = ..., generalized_statements: bool = ..., rdf_star: bool = ..., max_name_table_size: _Optional[int] = ..., max_prefix_table_size: _Optional[int] = ..., max_datatype_table_size: _Optional[int] = ..., logical_type: _Optional[_Union[LogicalStreamType, str]] = ..., version: _Optional[int] = ...) -> None: ...

class RdfStreamRow(_message.Message):
    __slots__ = ("options", "triple", "quad", "graph_start", "graph_end", "namespace", "name", "prefix", "datatype")
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TRIPLE_FIELD_NUMBER: _ClassVar[int]
    QUAD_FIELD_NUMBER: _ClassVar[int]
    GRAPH_START_FIELD_NUMBER: _ClassVar[int]
    GRAPH_END_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    options: RdfStreamOptions
    triple: RdfTriple
    quad: RdfQuad
    graph_start: RdfGraphStart
    graph_end: RdfGraphEnd
    namespace: RdfNamespaceDeclaration
    name: RdfNameEntry
    prefix: RdfPrefixEntry
    datatype: RdfDatatypeEntry
    def __init__(self, options: _Optional[_Union[RdfStreamOptions, _Mapping]] = ..., triple: _Optional[_Union[RdfTriple, _Mapping]] = ..., quad: _Optional[_Union[RdfQuad, _Mapping]] = ..., graph_start: _Optional[_Union[RdfGraphStart, _Mapping]] = ..., graph_end: _Optional[_Union[RdfGraphEnd, _Mapping]] = ..., namespace: _Optional[_Union[RdfNamespaceDeclaration, _Mapping]] = ..., name: _Optional[_Union[RdfNameEntry, _Mapping]] = ..., prefix: _Optional[_Union[RdfPrefixEntry, _Mapping]] = ..., datatype: _Optional[_Union[RdfDatatypeEntry, _Mapping]] = ...) -> None: ...

class RdfStreamFrame(_message.Message):
    __slots__ = ("rows", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    ROWS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[RdfStreamRow]
    metadata: _containers.ScalarMap[str, bytes]
    def __init__(self, rows: _Optional[_Iterable[_Union[RdfStreamRow, _Mapping]]] = ..., metadata: _Optional[_Mapping[str, bytes]] = ...) -> None: ...
