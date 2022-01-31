# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import abc
import base64
import logging
import re
import ssl
import types
import typing

import httpcore
import httpx
import spnego
import spnego.channel_bindings
import spnego.tls
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

from psrp._wsman._encryption import decrypt_wsman, encrypt_wsman

if typing.TYPE_CHECKING:
    from spnego._context import ContextProxy
    from spnego.channel_bindings import GssChannelBindings


log = logging.getLogger(__name__)

WWW_AUTH_PATTERN = re.compile(r"(CredSSP|Kerberos|Negotiate|NTLM)\s*([^,]*),?", re.I)


def get_tls_server_end_point_hash(
    certificate_der: bytes,
) -> bytes:
    """Get Channel Binding hash.

    Get the channel binding tls-server-end-point hash value from the
    certificate passed in.

    Args:
        certificate_der: The X509 DER encoded certificate.

    Returns:
        bytes: The hash value to use for the channel binding token.
    """
    backend = default_backend()

    cert = x509.load_der_x509_certificate(certificate_der, backend)
    try:
        hash_algorithm = cert.signature_hash_algorithm
    except UnsupportedAlgorithm:
        hash_algorithm = None

    # If the cert signature algorithm is unknown, md5, or sha1 then use sha256 otherwise use the signature
    # algorithm of the cert itself.
    if not hash_algorithm or hash_algorithm.name in ["md5", "sha1"]:
        digest = hashes.Hash(hashes.SHA256(), backend)
    else:
        digest = hashes.Hash(hash_algorithm, backend)

    digest.update(certificate_der)
    certificate_hash = digest.finalize()

    return certificate_hash


class AsyncResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream: typing.AsyncIterable[bytes]):
        self._stream = stream

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        async for part in self._stream:
            yield part

    async def aclose(self) -> None:
        if hasattr(self._stream, "aclose"):
            await self._stream.aclose()  # type: ignore[attr-defined] # hasattr check above


class AsyncResponseData(httpx.AsyncByteStream):
    def __init__(self, data: bytes):
        self._data = data

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield self._data

    async def aclose(self) -> None:
        pass


class AsyncWSManTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        url: httpx.URL,
        ssl_context: ssl.SSLContext,
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        protocol: str = "negotiate",
        encrypt: bool = True,
        service: str = "HTTP",
        hostname_override: typing.Optional[str] = None,
        disable_cbt: bool = False,
        delegate: bool = False,
        credssp_allow_tlsv1: bool = False,
        credssp_auth_mechanism: str = "negotiate",
    ) -> None:
        self._connection = httpcore.AsyncHTTPConnection(
            httpcore.Origin(url.raw_scheme, url.raw_host, url.port or 5985),
            ssl_context=ssl_context,
        )

        valid_protocols = ["kerberos", "negotiate", "ntlm", "credssp"]
        if protocol not in valid_protocols:
            raise ValueError(f"{type(self).__name__} protocol only supports {', '.join(valid_protocols)}")

        self.protocol = protocol.lower()

        self._auth_header = {
            "negotiate": "Negotiate",
            "ntlm": "Negotiate",
            "kerberos": "Kerberos",
            "credssp": "CredSSP",
        }[self.protocol]
        self._context: typing.Optional["ContextProxy"] = None
        self._username = username
        self._password = password
        self._encrypt = encrypt
        self._service = service
        self._hostname_override = hostname_override or url.host
        self._disable_cbt = disable_cbt
        self._channel_bindings: typing.Optional["GssChannelBindings"] = None
        self._delegate = delegate
        self._credssp_allow_tlsv1 = credssp_allow_tlsv1
        self._credssp_auth_mechanism = credssp_auth_mechanism.lower()

        if self._credssp_auth_mechanism not in ["kerberos", "negotiate", "ntlm"]:
            raise ValueError(
                f"{type(self).__name__} credssp_auth_mechanism must be set to kerberos, " "negotiate, or ntlm"
            )

    async def __aenter__(self) -> "AsyncWSManTransport":
        await self._connection.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: types.TracebackType = None,
    ) -> None:
        await self._connection.__aexit__()

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        if not self._context:
            auth_resp = await self._handle_async_auth(request)

            # If we didn't encrypt then the response from the auth phase contains our actual response. Also pass along
            # any errors back. Otherwise we need to drain the socket and read the dummy data to ensure the connection
            # is ready for the next request
            if not self._encrypt or auth_resp.status_code != 200:
                return auth_resp

            else:
                await auth_resp.aread()
                await auth_resp.aclose()

        req = self._wrap(request)

        resp = await self._connection.handle_async_request(req)

        return await self._unwrap(resp)

    async def _handle_async_auth(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        headers = request.headers.copy()
        stream: typing.Union[bytes, httpx.AsyncByteStream, httpx.SyncByteStream] = request.stream
        ext = request.extensions.copy()
        ext["trace"] = self.trace

        if self._encrypt:
            headers["Content-Length"] = "0"
            stream = b""

        out_token: typing.Optional[bytes] = None
        while True:
            if out_token:
                encoded_token = base64.b64encode(out_token).decode()
                headers["Authorization"] = f"{self._auth_header} {encoded_token}"
                out_token = None

            req = httpcore.Request(
                method=request.method,
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=headers.raw,
                content=stream,
                extensions=ext,
            )

            resp = await self._connection.handle_async_request(req)
            response = await self._unwrap(resp)

            if self._context:
                auths = response.headers.get("WWW-Authenticate", "")
                auth_header = WWW_AUTH_PATTERN.search(auths)
                in_token = base64.b64decode(auth_header.group(2)) if auth_header else None
                if in_token:
                    out_token = self._context.step(in_token)

            if out_token:
                await response.aread()
                await response.aclose()

            else:
                return response

    async def trace(
        self,
        event_name: str,
        info: typing.Dict[str, typing.Any],
    ) -> None:
        # print(f"{event_name} - {info!s}")

        normalized_name = event_name.lower().replace(".", "_")
        event_handler = getattr(self, f"_{normalized_name}", None)
        if event_handler:
            await event_handler(info)

    async def _http11_send_request_headers_started(self, info: typing.Dict[str, typing.Any]) -> None:
        # The first request needs the context to be set up and the first token added as a header
        if self._context:
            return

        auth_kwargs: typing.Dict[str, typing.Any] = {
            "username": self._username,
            "password": self._password,
            "hostname": self._hostname_override,
            "service": self._service,
            "context_req": spnego.ContextReq.default,
            "options": spnego.NegotiateOptions.none,
        }

        if self.protocol == "credssp":
            sub_auth_protocol = None
            if self._credssp_auth_mechanism == "kerberos":
                sub_auth_protocol = "kerberos"

            elif self._credssp_auth_mechanism == "ntlm":
                sub_auth_protocol = "ntlm"

            if sub_auth_protocol:
                sub_auth = spnego.client(protocol=sub_auth_protocol, **auth_kwargs)
                auth_kwargs["credssp_negotiate_context"] = sub_auth

            if self._credssp_allow_tlsv1:
                ssl_context = spnego.tls.default_tls_context()
                try:
                    ssl_context.context.minimum_version = ssl.TLSVersion.TLSv1
                except (AttributeError, ValueError):
                    ssl_context.context.options &= ~(ssl.Options.OP_NO_TLSv1 | ssl.Options.OP_NO_TLSv1_1)

                auth_kwargs["credssp_tls_context"] = ssl_context

        elif self._delegate:
            auth_kwargs["context_req"] |= spnego.ContextReq.delegate

        if self._encrypt:
            auth_kwargs["options"] |= spnego.NegotiateOptions.wrapping_winrm

        self._context = spnego.client(
            channel_bindings=self._channel_bindings,
            protocol=self.protocol,
            **auth_kwargs,
        )
        token = self._context.step() or b""
        encoded_token = base64.b64encode(token).decode()
        auth_value = f"{self._auth_header} {encoded_token}"
        info["request"].headers.append((b"Authorization", auth_value.encode()))

    async def _connection_start_tls_complete(self, info: typing.Dict[str, typing.Any]) -> None:
        # Once the TLS handshake is done we can immediately get the TLS channel bindings used later when creating the
        # auth context (as the headers have started).
        ssl_object = info["return_value"].get_extra_info("ssl_object")
        cert = ssl_object.getpeercert(True)
        cert_hash = get_tls_server_end_point_hash(cert)
        self._channel_bindings = spnego.channel_bindings.GssChannelBindings(
            application_data=b"tls-server-end-point:" + cert_hash
        )

    def _wrap(
        self,
        request: httpx.Request,
    ) -> httpcore.Request:
        if self._encrypt and self._context and self._context.complete:
            protocol = {
                "kerberos": "Kerberos",
                "credssp": "CredSSP",
            }.get(self.protocol, "SPNEGO")

            headers = request.headers

            data, content_type = encrypt_wsman(
                bytearray(request.content),
                headers["Content-Type"],
                f"application/HTTP-{protocol}-session-encrypted",
                self._context,
            )

            headers["Content-Type"] = content_type
            headers["Content-Length"] = str(len(data))

            return httpcore.Request(
                method=request.method,
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=headers.raw,
                content=data,
                extensions=request.extensions,
            )

        else:
            return httpcore.Request(
                method=request.method,
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=request.headers.raw,
                content=request.stream,
                extensions=request.extensions,
            )

    async def _unwrap(
        self,
        response: httpcore.Response,
    ) -> httpx.Response:
        headers = httpx.Headers(response.headers)
        content_type = headers.get("Content-Type", "")

        # A proxy will have these content types but cannot do the encryption so we must also check for self._encrypt.
        if (
            self._encrypt
            and self._context
            and self._context.complete
            and (
                content_type.startswith("multipart/encrypted;")
                or content_type.startswith("multipart/x-multi-encrypted;")
            )
        ):
            data = await response.aread()
            await response.aclose()

            data, content_type = decrypt_wsman(bytearray(data), content_type, self._context)
            headers["Content-Length"] = str(len(data))
            headers["Content-Type"] = content_type

            return httpx.Response(
                status_code=response.status,
                headers=headers,
                stream=AsyncResponseData(data),
                extensions=response.extensions,
            )

        else:
            return httpx.Response(
                status_code=response.status,
                headers=headers,
                stream=AsyncResponseStream(response.stream),
                extensions=response.extensions,
            )


class _WSManConnectionBase(metaclass=abc.ABCMeta):
    """The WSManConnection contract.

    This is the WSManConnection contract that defines what is required for a WSMan IO class to be used by this library.
    """

    _IS_ASYNC = False

    def __init__(
        self,
        connection_uri: str,
        encryption: str = "auto",
        verify: typing.Union[str, bool] = True,
        connection_timeout: int = 30,
        read_timeout: int = 30,
        # TODO: reconnection settings
        # Proxy settings
        proxy: typing.Optional[str] = None,
        proxy_username: typing.Optional[str] = None,
        proxy_password: typing.Optional[str] = None,
        proxy_auth: typing.Optional[str] = None,
        proxy_service: typing.Optional[str] = "HTTP",
        proxy_hostname: typing.Optional[str] = None,
        auth: str = "negotiate",
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        # Cert auth
        certificate_pem: typing.Optional[str] = None,
        certificate_key_pem: typing.Optional[str] = None,
        certificate_password: typing.Optional[str] = None,
        # SPNEGO
        negotiate_service: str = "HTTP",
        negotiate_hostname: typing.Optional[str] = None,
        negotiate_delegate: bool = False,
        send_cbt: bool = True,
        # CredSSP
        credssp_allow_tlsv1: bool = False,
        credssp_auth_mechanism: str = "negotiate",
    ):
        self.connection_uri = httpx.URL(connection_uri)

        if encryption not in ["auto", "always", "never"]:
            raise ValueError("The encryption value '%s' must be auto, always, or never" % encryption)

        encrypt = {
            "auto": self.connection_uri.scheme == "http",
            "always": True,
            "never": False,
        }[encryption]

        # Default for 'Accept-Encoding' is 'gzip, default' which normally doesn't matter on vanilla WinRM but for
        # Exchange endpoints hosted on IIS they actually compress it with 1 of the 2 algorithms. By explicitly setting
        # identity we are telling the server not to transform (compress) the data using the HTTP methods which we don't
        # support. https://tools.ietf.org/html/rfc7231#section-5.3.4
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": "Python PSRP Client",
        }
        ssl_context = httpx.create_ssl_context(verify=verify)

        transport: typing.Optional[httpx.AsyncBaseTransport] = None
        auth_handler: typing.Optional[httpx.Auth] = None

        auth = auth.lower()
        if auth == "basic":
            auth_handler = httpx.BasicAuth(username or "", password or "")

            if encrypt:
                raise ValueError("Cannot encrypt without auth encryption")

        elif auth == "certificate":
            headers["Authorization"] = "http://schemas.dmtf.org/wbem/wsman/1/wsman/secprofile/https/mutual"
            ssl_context.load_cert_chain(
                certfile=certificate_pem, keyfile=certificate_key_pem, password=certificate_password
            )

            if encrypt:
                raise ValueError("Cannot encrypt without auth encryption")

        elif auth in ["credssp", "kerberos", "negotiate", "ntlm"]:
            transport = AsyncWSManTransport(
                url=self.connection_uri,
                ssl_context=ssl_context,
                username=username,
                password=password,
                protocol=auth,
                encrypt=encrypt,
                service=negotiate_service,
                hostname_override=negotiate_hostname,
                disable_cbt=not send_cbt,
                delegate=negotiate_delegate,
                credssp_allow_tlsv1=credssp_allow_tlsv1,
                credssp_auth_mechanism=credssp_auth_mechanism,
            )

        else:
            raise ValueError("Invalid auth specified")

        proxy_handler: typing.Optional[httpx.Proxy] = None
        if proxy:
            proxy_handler = httpx.Proxy(proxy)

        # proxy_auth = proxy_auth.lower() if proxy_auth else None
        # if proxy_auth == "basic":
        #     proxy_auth = BasicAuth(proxy_username, proxy_password)

        # elif proxy_auth in ["kerberos", "negotiate", "ntlm"]:
        #     proxy_auth = NegotiateAuth(
        #         credential=(proxy_username, proxy_password),
        #         protocol=proxy_auth,
        #         encrypt=False,
        #         service=proxy_service,
        #         hostname_override=proxy_hostname,
        #     )

        # elif proxy_auth is None or proxy_auth == "none":
        #     proxy_auth = None

        # else:
        #     raise ValueError("Invalid proxy_auth specified")

        timeout = httpx.Timeout(max(connection_timeout, read_timeout), connect=connection_timeout, read=read_timeout)
        client_type = httpx.AsyncClient if self._IS_ASYNC else httpx.Client
        self._http = client_type(
            headers=headers,
            timeout=timeout,
            transport=transport,
            auth=auth_handler,
            proxies=proxy_handler,
        )

    async def __aenter__(self):
        """Implements 'async with' for the WSMan connection."""
        await self.open()
        return self

    def __enter__(self):
        """Implements 'with' for the WSMan connection."""
        self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Implements the closing method for 'async with' for the WSMan connection."""
        await self.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Implements the closing method for 'with' for the WSMan connection."""
        self.close()

    @abc.abstractmethod
    def send(
        self,
        data: bytes,
    ) -> bytes:
        """Send WSMan data to the endpoint.

        The WSMan envelope is sent as a HTTP POST request to the endpoint specified. This method should deal with the
        encryption required for a request if it is necessary.

        Args:
            data: The WSMan envelope to send to the endpoint.

        Returns:
            bytes: The WSMan response.
        """
        pass

    @abc.abstractmethod
    def open(self):
        """Opens the WSMan connection.

        Opens the WSMan connection and sets up the connection for sending any WSMan envelopes.
        """
        pass

    @abc.abstractmethod
    def close(self):
        """Closes the WSMan connection.

        Closes the WSMan connection and any sockets/connections that are in use.
        """
        pass


class AsyncWSManConnection(_WSManConnectionBase):
    _IS_ASYNC = True

    async def send(
        self,
        data: bytes,
    ) -> bytes:
        response = await self._http.post(
            self.connection_uri,
            content=data,
            headers={
                "Content-Type": "application/soap+xml;charset=UTF-8",
            },
        )

        content = await response.aread()

        # A WSManFault has more information that the WSMan state machine can
        # handle with better context so we ignore those.
        if response.status_code != 200 and (not content or b"wsmanfault" not in content):
            response.raise_for_status()

        return content

    async def open(self):
        await self._http.__aenter__()

    async def close(self):
        await self._http.aclose()


class WSManConnection(_WSManConnectionBase):
    _IS_ASYNC = False

    def send(
        self,
        data: bytes,
    ) -> bytes:
        log.debug("WSMan Request", data.decode())
        response = self._http.post(
            self.connection_uri.geturl(),
            content=data,
            headers={
                "Content-Type": "application/soap+xml;charset=UTF-8",
            },
        )

        content = response.read()
        if content:
            log.debug("WSMan Response", content.decode())

        # A WSManFault has more information that the WSMan state machine can
        # handle with better context so we ignore those.
        if response.status_code != 200 and (not content or b"wsmanfault" not in content):
            response.raise_for_status()

        return content

    def open(self):
        self._http.__enter__()

    def close(self):
        self._http.close()
