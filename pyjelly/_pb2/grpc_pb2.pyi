import rdf_pb2 as _rdf_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RdfStreamSubscribe(_message.Message):
    __slots__ = ("topic", "requested_options")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    topic: str
    requested_options: _rdf_pb2.RdfStreamOptions
    def __init__(self, topic: _Optional[str] = ..., requested_options: _Optional[_Union[_rdf_pb2.RdfStreamOptions, _Mapping]] = ...) -> None: ...

class RdfStreamReceived(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
