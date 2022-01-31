# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import http
import logging
import ssl
import typing

import httpx
from httpcore import (
    AsyncByteStream,
    AsyncHTTPTransport,
    ByteStream,
    SyncByteStream,
    SyncHTTPTransport,
)

from ._auth import AuthHandler, NoAuth
from ._backends import AsyncioBackend, SyncBackend

logger = logging.getLogger(__name__)

URL = typing.Tuple[bytes, bytes, typing.Optional[int], bytes]
Headers = typing.List[typing.Tuple[bytes, bytes]]


class WSManTransport(AsyncHTTPTransport, SyncHTTPTransport):
    def __init__(
        self,
        is_async: bool,
        auth: typing.Optional[AuthHandler] = None,
        ssl_context: ssl.SSLContext = None,
        keepalive_expiry: float = 60.0,
        proxy_url: typing.Optional[str] = None,
        proxy_auth: typing.Optional[AuthHandler] = None,
    ):
        # Connection options
        self._backend = AsyncioBackend() if is_async else SyncBackend()
        self._connection = None
        self._auth = auth or NoAuth()
        self._ssl_context = ssl_context
        self._keepalive_expiry = keepalive_expiry

        # Proxy options
        self._proxy_url = httpx.URL(proxy_url) if proxy_url else None
        self._proxy_auth = proxy_auth or NoAuth()

    def handle_request(
        self,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: SyncByteStream,
        extensions: typing.Dict,
    ) -> typing.Tuple[int, Headers, SyncByteStream, dict]:
        connection = self._get_connection(url, extensions)
        return self._auth.handle_request(connection, method, url, headers, stream, extensions)

    async def handle_async_request(
        self,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: AsyncByteStream,
        extensions: typing.Dict,
    ) -> typing.Tuple[int, Headers, AsyncByteStream, typing.Dict]:
        connection = await self._get_async_connection(url, extensions)
        return await self._auth.handle_async_request(connection, method, url, headers, stream, extensions)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    async def aclose(self) -> None:
        if self._connection:
            await self._connection.aclose()
            self._connection = None

    def _create_connection(
        self,
        url: URL,
        proxy_url: typing.Optional[httpx.URL],
        extensions: typing.Dict,
    ) -> SyncHTTPTransport:
        timeout = extensions.get("timeout", {})

        if not proxy_url:
            sock = self._backend.httpcore_backend.open_tcp_stream(
                hostname=url[1],
                port=url[2],
                ssl_context=(self._ssl_context if url[0] == b"https" else None),
                timeout=timeout,
                local_address=None,
            )
            return self._backend.new_connection(url[:3], sock, self._ssl_context)

        if proxy_url.scheme in ["socks5", "socks5h"]:
            return self._backend.new_socks_connection(proxy_url, httpx.URL(url), timeout, self._ssl_context)

        proxy_url = proxy_url.raw
        connection = self._create_connection(proxy_url, None, extensions)

        try:
            if url[0] == b"http":
                return ProxyConnection(proxy_url, connection, auth=self._proxy_auth)

            # CONNECT
            target = b"%b:%d" % (url[1], url[2])
            connect_url = proxy_url[:3] + (target,)
            connect_headers = [(b"Host", target), (b"Accept", b"*/*")]

            proxy_status_code = (
                self._proxy_auth.handle_request(
                    connection,
                    b"CONNECT",
                    connect_url,
                    connect_headers,
                    ByteStream(b""),
                    extensions,
                    auths_header="Proxy-Authenticate",
                    authz_header="Proxy-Authorization",
                )
            )[0]

            if proxy_status_code < 200 or proxy_status_code > 299:
                try:
                    reason = http.HTTPStatus(proxy_status_code).phrase
                except ValueError:
                    reason = ""
                raise Exception(f"Proxy failed {proxy_status_code} {reason}")

            # Do start_tls on the connection.
            connection = self._backend.start_tls(url[:3], connection.socket, self._ssl_context, timeout)

        except Exception:
            connection.socket.close()
            raise

        return connection

    async def _create_async_connection(
        self,
        url: URL,
        proxy_url: typing.Optional[httpx.URL],
        extensions: typing.Dict,
    ) -> AsyncHTTPTransport:
        timeout = extensions.get("timeout", {})

        if not proxy_url:
            sock = await self._backend.httpcore_backend.open_tcp_stream(
                hostname=url[1],
                port=url[2],
                ssl_context=(self._ssl_context if url[0] == b"https" else None),
                timeout=timeout,
                local_address=None,
            )
            return await self._backend.new_connection(url[:3], sock, self._ssl_context)

        if proxy_url.scheme in ["socks5", "socks5h"]:
            return await self._backend.new_socks_connection(proxy_url, httpx.URL(url), timeout, self._ssl_context)

        proxy_url = proxy_url.raw
        connection = await self._create_async_connection(proxy_url, None, extensions)

        try:
            if url[0] == b"http":
                return ProxyConnection(proxy_url, connection, auth=self._proxy_auth)

            # CONNECT
            target = b"%b:%d" % (url[1], url[2])
            connect_url = proxy_url[:3] + (target,)
            connect_headers = [(b"Host", target), (b"Accept", b"*/*")]

            proxy_status_code = (
                await self._proxy_auth.handle_async_request(
                    connection,
                    b"CONNECT",
                    connect_url,
                    connect_headers,
                    ByteStream(b""),
                    extensions,
                    auths_header="Proxy-Authenticate",
                    authz_header="Proxy-Authorization",
                )
            )[0]

            if proxy_status_code < 200 or proxy_status_code > 299:
                try:
                    reason = http.HTTPStatus(proxy_status_code).phrase
                except ValueError:
                    reason = ""
                raise Exception(f"Proxy failed {proxy_status_code} {reason}")

            # Do start_tls on the connection.
            connection = await self._backend.start_tls(url[:3], connection.socket, self._ssl_context, timeout)
            a = ""

        except Exception:
            await connection.socket.aclose()
            raise

        return connection

    def _get_connection(
        self,
        url: URL,
        extensions: typing.Dict,
    ) -> SyncHTTPTransport:
        # FIXME: Add is alive checks
        if self._connection:
            a = ""

        if not self._connection:
            self._auth.reset()
            self._connection = self._create_connection(url, self._proxy_url, extensions)

        return self._connection

    async def _get_async_connection(
        self,
        url: URL,
        extensions: typing.Dict,
    ) -> AsyncHTTPTransport:
        # FIXME: Add is alive checks
        if self._connection:
            a = ""

        if not self._connection:
            self._auth.reset()
            self._connection = await self._create_async_connection(url, self._proxy_url, extensions)

        return self._connection


class ProxyConnection(AsyncHTTPTransport, SyncHTTPTransport):
    def __init__(
        self,
        proxy_url: URL,
        connection: typing.Union[AsyncHTTPTransport, SyncHTTPTransport],
        auth: AuthHandler = None,
    ):
        self.proxy_url = proxy_url
        self._connection = connection
        self._auth = auth or NoAuth()

    def handle_request(
        self,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: SyncByteStream,
        extensions: typing.Dict,
    ) -> typing.Tuple[int, Headers, SyncByteStream, dict]:
        return self._auth.handle_request(
            self._connection,
            method,
            self._proxify(url),
            headers,
            stream,
            extensions,
            auths_header="Proxy-Authenticate",
            authz_header="Proxy-Authorization",
        )

    async def handle_async_request(
        self,
        method: bytes,
        url: URL,
        headers: Headers,
        stream: AsyncByteStream,
        extensions: typing.Dict,
    ) -> typing.Tuple[int, Headers, AsyncByteStream, typing.Dict]:
        return await self._auth.handle_async_request(
            self._connection,
            method,
            self._proxify(url),
            headers,
            stream,
            extensions,
            auths_header="Proxy-Authenticate",
            authz_header="Proxy-Authorization",
        )

    def close(self) -> None:
        self._connection.close()

    async def aclose(self) -> None:
        await self._connection.aclose()

    def _proxify(
        self,
        url: URL,
    ) -> URL:
        scheme, host, port, path = url
        if port is None:
            target = b"%s://%b%b" % (scheme, host, path)
        else:
            target = b"%b://%b:%d%b" % (scheme, host, port, path)

        return self.proxy_url[:3] + (target,)
