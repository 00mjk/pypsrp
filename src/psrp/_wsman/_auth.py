# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import base64
import functools
import logging
import re
import ssl
import typing

import httpx
import spnego
import spnego.channel_bindings
import spnego.tls
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from httpcore import (
    AsyncByteStream,
    AsyncHTTPTransport,
    ByteStream,
    SyncByteStream,
    SyncHTTPTransport,
)

from ._encryption import decrypt_wsman, encrypt_wsman

logger = logging.getLogger(__name__)

URL = typing.Tuple[bytes, bytes, typing.Optional[int], bytes]
Headers = typing.List[typing.Tuple[bytes, bytes]]

WWW_AUTH_PATTERN = re.compile(r"(CredSSP|Kerberos|Negotiate|NTLM)\s*([^,]*),?", re.I)
WWW_AUTHS = "WWW-Authenticate"
WWW_AUTHZ = "Authorization"


def _async_wrap(func, *args, **kwargs):
    """Runs a sync function in the background."""
    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

    return task


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


class _AuthBase:
    """Base authentication handler for the WSMan transports."""

    SUPPORTS_ENCRYPTION = False

    def handle_request(
        self,
        connection: SyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: SyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, SyncByteStream, typing.Dict]:
        return connection.handle_request(method, url, headers, stream, extensions)

    async def handle_async_request(
        self,
        connection: AsyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: AsyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, AsyncByteStream, typing.Dict]:
        return await connection.handle_async_request(method, url, headers, stream, extensions)

    def reset(self):
        pass


class NoAuth(_AuthBase):
    """Authentication handler that is no-op and doesn't provide any authentication on HTTP requests."""


class NegotiateAuth(_AuthBase):

    SUPPORTS_ENCRYPTION = True

    def __init__(
        self,
        credential: typing.Any = None,
        protocol: str = "negotiate",
        encrypt: bool = True,
        service: str = "HTTP",
        hostname_override: typing.Optional[str] = None,
        disable_cbt: bool = False,
        delegate: bool = False,
        credssp_allow_tlsv1: bool = False,
        credssp_auth_mechanism: str = "negotiate",
    ):
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
        self._context = None
        self.__complete = False
        self._credential = credential
        self._encrypt = encrypt
        self._service = service
        self._hostname_override = hostname_override
        self._disable_cbt = disable_cbt
        self._delegate = delegate
        self._credssp_allow_tlsv1 = credssp_allow_tlsv1
        self._credssp_auth_mechanism = credssp_auth_mechanism.lower()

        if self._credssp_auth_mechanism not in ["kerberos", "negotiate", "ntlm"]:
            raise ValueError(
                f"{type(self).__name__} credssp_auth_mechanism must be set to kerberos, " "negotiate, or ntlm"
            )

    @property
    def _complete(self) -> bool:
        """Whether the authentication process is complete."""
        return self.__complete or (self._context and self._context.complete)

    def handle_request(
        self,
        connection: SyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: SyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, SyncByteStream, typing.Dict]:
        headers = httpx.Headers(headers)

        if not self._complete:
            resp = self._handle_auth(
                connection, method, url, headers.copy(), stream, extensions, auths_header, authz_header
            )

            # If we didn't encrypt then the response from the auth phase contains our actual response. Also pass along
            # any errors back. Otherwise we need to drain the socket and read the dummy data.
            if not self._encrypt or resp[0] != 200:
                return resp
            else:
                _ = resp[2].read()

        if self._encrypt:
            headers, stream = self._wrap(headers, stream.read())

        status_code, headers, stream, extensions = connection.handle_request(
            method, url, headers.raw, stream, extensions
        )
        headers = httpx.Headers(headers)

        if self._unwrap_required(headers):
            headers, stream = self._unwrap(headers, stream.read())

        return status_code, headers.raw, stream, extensions

    def _handle_auth(
        self,
        connection: SyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: httpx.Headers,
        stream: SyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, SyncByteStream, typing.Dict]:
        self._context = self._build_context(connection, url[1])

        if self._encrypt:
            headers["Content-Length"] = "0"
            stream = ByteStream(b"")

        status_code = resp_headers = resp_stream = in_token = None
        while not self._context.complete:
            out_token = self._context.step(in_token)
            if not out_token:
                break

            if resp_stream:
                _ = resp_stream.read()

            self._add_header(headers, out_token, authz_header)
            status_code, resp_headers, resp_stream, extensions = connection.handle_request(
                method, url, headers.raw, stream, extensions
            )
            resp_headers = httpx.Headers(resp_headers)

            in_token = self._get_header_token(resp_headers, auths_header)
            if not in_token:
                break

        return status_code or 500, resp_headers or [], resp_stream or ByteStream(b""), extensions

    async def handle_async_request(
        self,
        connection: AsyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: AsyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, AsyncByteStream, typing.Dict]:
        headers = httpx.Headers(headers)

        if not self._complete:
            resp = await self._handle_async_auth(
                connection, method, url, headers.copy(), stream, extensions, auths_header, authz_header
            )

            # If we didn't encrypt then the response from the auth phase contains our actual response. Also pass along
            # any errors back. Otherwise we need to drain the socket and read the dummy data.
            if not self._encrypt or resp[0] != 200:
                return resp
            else:
                _ = await resp[2].aread()

        if self._encrypt:
            headers, stream = self._wrap(headers, await stream.aread())

        status_code, headers, stream, extensions = await connection.handle_async_request(
            method, url, headers.raw, stream, extensions
        )
        headers = httpx.Headers(headers)

        if self._unwrap_required(headers):
            headers, stream = self._unwrap(headers, await stream.aread())

        return status_code, headers.raw, stream, extensions

    async def _handle_async_auth(
        self,
        connection: AsyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: httpx.Headers,
        stream: AsyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, AsyncByteStream, typing.Dict]:
        self._context = await _async_wrap(self._build_context, connection, url[1])

        if self._encrypt:
            headers["Content-Length"] = "0"
            stream = ByteStream(b"")

        status_code = resp_headers = resp_stream = in_token = None
        while not self._context.complete:
            out_token = await _async_wrap(self._context.step, in_token)
            if not out_token:
                break

            if resp_stream:
                _ = await resp_stream.aread()

            self._add_header(headers, out_token, authz_header)
            status_code, resp_headers, resp_stream, extensions = await connection.handle_async_request(
                method, url, headers.raw, stream, extensions
            )
            resp_headers = httpx.Headers(resp_headers)

            in_token = self._get_header_token(resp_headers, auths_header)
            if not in_token:
                break

        return status_code or 500, resp_headers or [], resp_stream or ByteStream(b""), extensions

    def reset(self):
        self._context = None
        self.__complete = False

    def _build_context(
        self,
        connection: typing.Union[AsyncHTTPTransport, SyncHTTPTransport],
        hostname: bytes,
    ):
        cbt = None
        ssl_object = None
        if hasattr(connection, "socket"):
            if hasattr(connection.socket, "stream_writer"):
                # asyncio
                ssl_object = connection.socket.stream_writer.get_extra_info("ssl_object")

            elif hasattr(connection.socket.sock, "getpeercert"):
                # sync
                ssl_object = connection.socket.sock

        if ssl_object and not self._disable_cbt and self.protocol != "credssp":
            cert = ssl_object.getpeercert(True)
            cert_hash = get_tls_server_end_point_hash(cert)
            cbt = spnego.channel_bindings.GssChannelBindings(application_data=b"tls-server-end-point:" + cert_hash)

        username, password = self._credential
        auth_kwargs = {
            "username": username,
            "password": password,
            "hostname": self._hostname_override or hostname.decode("utf-8"),
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

        return spnego.client(
            channel_bindings=cbt,
            protocol=self.protocol,
            **auth_kwargs,
        )

    def _add_header(self, headers: httpx.Headers, token: bytes, authz_header: str):
        headers[authz_header] = f"{self._auth_header} {base64.b64encode(token).decode()}"

    def _get_header_token(
        self,
        headers: httpx.Headers,
        auths_header: str,
    ) -> typing.Optional[bytes]:
        auths = headers.get(auths_header, "")
        in_token = WWW_AUTH_PATTERN.search(auths)
        if in_token:
            in_token = base64.b64decode(in_token.group(2))

        if not in_token:
            # Some proxies don't seem to return the mutual auth token which
            # break the _context.complete checker later on. Because mutual
            # auth doesn't matter for proxies we just override that check.
            self.__complete = True
            in_token = None

        return in_token

    def _wrap(
        self,
        headers: httpx.Headers,
        data: bytes,
    ) -> typing.Tuple[httpx.Headers, ByteStream]:
        protocol = {
            "kerberos": "Kerberos",
            "credssp": "CredSSP",
        }.get(self.protocol, "SPNEGO")

        data, content_type = encrypt_wsman(
            bytearray(data),
            headers["Content-Type"],
            f"application/HTTP-{protocol}-session-encrypted",
            self._context,
        )
        headers["Content-Type"] = content_type
        headers["Content-Length"] = str(len(data))

        return headers, ByteStream(data)

    def _unwrap(
        self,
        headers: httpx.Headers,
        data: bytes,
    ) -> typing.Tuple[httpx.Headers, ByteStream]:
        content_type = headers["Content-Type"]

        data, content_type = decrypt_wsman(bytearray(data), content_type, self._context)
        headers["Content-Length"] = str(len(data))
        headers["Content-Type"] = content_type

        return headers, ByteStream(data)

    def _unwrap_required(
        self,
        headers: httpx.Headers,
    ) -> bool:
        """Checks whether the response stream needs unwrapping."""
        content_type = headers.get("Content-Type", "")

        # A proxy will have these content types but cannot do the encryption so we must also check for self._encrypt.
        return self._encrypt and (
            content_type.startswith("multipart/encrypted;") or content_type.startswith("multipart/x-multi-encrypted;")
        )


class BasicAuth(_AuthBase):
    def __init__(
        self,
        username: str,
        password: str,
    ):
        credential = f'{username or ""}:{password or ""}'.encode("utf-8")
        self._token = f"Basic {base64.b64encode(credential).decode()}"

    def _add_header(
        self,
        headers: Headers,
        authz_header: str,
    ) -> Headers:
        headers = httpx.Headers(headers)
        headers[authz_header] = self._token

        return headers.raw

    def handle_request(
        self,
        connection: SyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: SyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, SyncByteStream, typing.Dict]:
        headers = self._add_header(headers, authz_header)

        return connection.handle_request(method, url, headers, stream, extensions)

    async def handle_async_request(
        self,
        connection: AsyncHTTPTransport,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: AsyncByteStream,
        extensions: typing.Dict,
        auths_header: str = WWW_AUTHS,
        authz_header: str = WWW_AUTHZ,
    ) -> typing.Tuple[int, Headers, AsyncByteStream, typing.Dict]:
        headers = self._add_header(headers, authz_header)

        return await connection.handle_async_request(method, url, headers, stream, extensions)


AuthHandler = typing.TypeVar("AuthHandler", bound=_AuthBase)
