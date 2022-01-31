import typing

from spnego._context import ContextProxy, ContextReq
from spnego.channel_bindings import GssChannelBindings
from spnego.exceptions import NegotiateOptions

def client(
    username: typing.Optional[str] = None,
    password: typing.Optional[str] = None,
    hostname: str = "unspecified",
    service: str = "host",
    channel_bindings: typing.Optional[GssChannelBindings] = None,
    context_req: ContextReq = ContextReq.default,
    protocol: str = "negotiate",
    options: NegotiateOptions = NegotiateOptions.none,
    **kwargs: typing.Any,
) -> ContextProxy: ...
