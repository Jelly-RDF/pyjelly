from pyjelly.jelly import rdf_pb2 as _rdf_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SparqlStreamType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SPARQL_STREAM_TYPE_FLAT: _ClassVar[SparqlStreamType]
    SPARQL_STREAM_TYPE_PUNCTUATED: _ClassVar[SparqlStreamType]
SPARQL_STREAM_TYPE_FLAT: SparqlStreamType
SPARQL_STREAM_TYPE_PUNCTUATED: SparqlStreamType

class SparqlResultsOptions(_message.Message):
    __slots__ = ("stream_name", "stream_type", "rdf_version", "max_name_table_size", "max_prefix_table_size", "max_datatype_table_size", "version")
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    STREAM_TYPE_FIELD_NUMBER: _ClassVar[int]
    RDF_VERSION_FIELD_NUMBER: _ClassVar[int]
    MAX_NAME_TABLE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_PREFIX_TABLE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_DATATYPE_TABLE_SIZE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    stream_name: str
    stream_type: SparqlStreamType
    rdf_version: _rdf_pb2.RdfVersion
    max_name_table_size: int
    max_prefix_table_size: int
    max_datatype_table_size: int
    version: int
    def __init__(self, stream_name: _Optional[str] = ..., stream_type: _Optional[_Union[SparqlStreamType, str]] = ..., rdf_version: _Optional[_Union[_rdf_pb2.RdfVersion, str]] = ..., max_name_table_size: _Optional[int] = ..., max_prefix_table_size: _Optional[int] = ..., max_datatype_table_size: _Optional[int] = ..., version: _Optional[int] = ...) -> None: ...

class SparqlAskResult(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bool
    def __init__(self, value: bool = ...) -> None: ...

class SparqlResultsTrailer(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class SparqlResultsFrame(_message.Message):
    __slots__ = ("options", "variables", "row_count", "names", "prefixes", "datatypes", "columns", "ask_result", "trailer", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    VARIABLES_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    NAMES_FIELD_NUMBER: _ClassVar[int]
    PREFIXES_FIELD_NUMBER: _ClassVar[int]
    DATATYPES_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ASK_RESULT_FIELD_NUMBER: _ClassVar[int]
    TRAILER_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    options: SparqlResultsOptions
    variables: _containers.RepeatedScalarFieldContainer[str]
    row_count: int
    names: _containers.RepeatedCompositeFieldContainer[_rdf_pb2.RdfLookupEntryPacked]
    prefixes: _containers.RepeatedCompositeFieldContainer[_rdf_pb2.RdfLookupEntryPacked]
    datatypes: _containers.RepeatedCompositeFieldContainer[_rdf_pb2.RdfLookupEntryPacked]
    columns: _containers.RepeatedCompositeFieldContainer[_rdf_pb2.RdfColumn]
    ask_result: SparqlAskResult
    trailer: SparqlResultsTrailer
    metadata: _containers.ScalarMap[str, bytes]
    def __init__(self, options: _Optional[_Union[SparqlResultsOptions, _Mapping]] = ..., variables: _Optional[_Iterable[str]] = ..., row_count: _Optional[int] = ..., names: _Optional[_Iterable[_Union[_rdf_pb2.RdfLookupEntryPacked, _Mapping]]] = ..., prefixes: _Optional[_Iterable[_Union[_rdf_pb2.RdfLookupEntryPacked, _Mapping]]] = ..., datatypes: _Optional[_Iterable[_Union[_rdf_pb2.RdfLookupEntryPacked, _Mapping]]] = ..., columns: _Optional[_Iterable[_Union[_rdf_pb2.RdfColumn, _Mapping]]] = ..., ask_result: _Optional[_Union[SparqlAskResult, _Mapping]] = ..., trailer: _Optional[_Union[SparqlResultsTrailer, _Mapping]] = ..., metadata: _Optional[_Mapping[str, bytes]] = ...) -> None: ...
