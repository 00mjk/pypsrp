import enum
import typing

class BufferType(enum.IntEnum):
    empty = enum.auto()
    data = enum.auto()
    header = enum.auto()
    pkg_params = enum.auto()
    trailer = enum.auto()
    padding = enum.auto()
    stream = enum.auto()
    sign_only = enum.auto()
    mic_token = enum.auto()

class IOVBuffer(typing.NamedTuple):
    type: BufferType
    data: typing.Union[bytes, int, bool]
