import enum
import typing

from spnego.channel_bindings import GssChannelBindings
from spnego.exceptions import NegotiateOptions
from spnego.iov import IOVBuffer

class WrapResult(typing.NamedTuple):
    data: bytes
    encrypted: bool

class IOVWrapResult(typing.NamedTuple):
    buffer: typing.Tuple[IOVBuffer, ...]
    encrypted: bool

class WinRMWrapResult(typing.NamedTuple):
    header: bytes
    data: bytes
    padding_length: int

class UnwrapResult(typing.NamedTuple):
    data: bytes
    encrypted: bool
    qop: int

class IOVUnwrapResult(typing.NamedTuple):
    buffers: typing.Tuple[IOVBuffer, ...]
    encrypted: bool
    qop: int

class ContextReq(enum.IntFlag):
    none = enum.auto()
    delegate = enum.auto()
    mutual_auth = enum.auto()
    replay_detect = enum.auto()
    sequence_detect = enum.auto()
    confidentiality = enum.auto()
    integrity = enum.auto()
    identify = enum.auto()
    delegate_policy = enum.auto()
    default = enum.auto()

class ContextProxy:
    def __init__(
        self,
        username: typing.Optional[str],
        password: typing.Optional[str],
        hostname: typing.Optional[str],
        service: typing.Optional[str],
        channel_bindings: typing.Optional[GssChannelBindings],
        context_req: ContextReq,
        usage: str,
        protocol: str,
        options: NegotiateOptions,
        _is_wrapped: bool = False,
    ) -> None: ...
    @classmethod
    def available_protocols(cls, options: typing.Optional[NegotiateOptions] = None) -> typing.List[str]: ...
    @classmethod
    def iov_available(cls) -> bool: ...
    @property
    def client_principal(self) -> typing.Optional[str]: ...
    @property
    def complete(self) -> bool: ...
    @property
    def context_attr(self) -> ContextReq: ...
    @property
    def negotiated_protocol(self) -> typing.Optional[str]: ...
    @property
    def session_key(self) -> bytes: ...
    def step(self, in_token: typing.Optional[bytes] = None) -> typing.Optional[bytes]: ...
    def wrap(self, data: bytes, encrypt: bool = True, qop: typing.Optional[int] = None) -> WrapResult: ...
    def wrap_iov(
        self,
        iov: typing.List[IOVBuffer],
        encrypt: bool = True,
        qop: typing.Optional[int] = None,
    ) -> IOVWrapResult: ...
    def wrap_winrm(self, data: bytes) -> WinRMWrapResult: ...
    def unwrap(self, data: bytes) -> UnwrapResult: ...
    def unwrap_iov(self, iov: typing.List[IOVBuffer]) -> IOVUnwrapResult: ...
    def unwrap_winrm(self, header: bytes, data: bytes) -> bytes: ...
    def sign(self, data: bytes, qop: typing.Optional[int] = None) -> bytes: ...
    def verify(self, data: bytes, mic: bytes) -> int: ...
