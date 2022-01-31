# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import socket
import ssl
import typing

import httpx
from httpcore._async.connection import AsyncHTTPConnection
from httpcore._backends.asyncio import AsyncioBackend as _AsyncioBackend
from httpcore._backends.asyncio import SocketStream
from httpcore._backends.sync import SyncBackend as _SyncBackend
from httpcore._backends.sync import SyncSocketStream
from httpcore._sync.connection import SyncHTTPConnection

HAS_SOCKS = True
try:
    from python_socks.async_.asyncio import Proxy as AsyncSocks
    from python_socks.sync import Proxy as SyncSocks
except ImportError:
    HAS_SOCKS = False


# FUTURE: Look into an AnyioBackend
# FUTURE: Talk to httpcore to export the wrap_sock as part of the backend.
# FUTURE: Implement fixes for start_tls in asyncio backend.


class AsyncioBackend:
    def __init__(self):
        self.httpcore_backend = _AsyncioBackend()

    async def new_connection(
        self,
        origin: typing.Tuple[bytes, bytes, typing.Optional[int]],
        sock: SocketStream,
        ssl_context: typing.Optional[ssl.SSLContext],
    ) -> AsyncHTTPConnection:
        return AsyncHTTPConnection(
            origin,
            socket=sock,
            ssl_context=ssl_context,
            backend=self.httpcore_backend,
        )

    async def new_socks_connection(
        self,
        proxy_url: httpx.URL,
        target_url: httpx.URL,
        timeout: typing.Dict,
        ssl_context: typing.Optional[ssl.SSLContext],
    ) -> SyncHTTPConnection:
        if not HAS_SOCKS:
            raise ImportError("Need pypsrp[socks] to be installed")

        # python-socks doesn't natively understand socks5h, we adjust the prefix and set rdns based on whether socks5h is
        # set or not.
        scheme = proxy_url.scheme
        proxy_url = str(proxy_url)
        rdns = False
        if scheme == "socks5h":
            rdns = True
            proxy_url = proxy_url.replace("socks5h://", "socks5://", 1)

        connect_timeout = timeout.get("connect", 60)
        proxy = AsyncSocks.from_url(proxy_url, rdns=rdns)
        sock = await proxy.connect(dest_host=target_url.host, dest_port=target_url.port, timeout=connect_timeout)

        if target_url.scheme == "https":
            server_hostname = target_url.host
        else:
            server_hostname = None
            ssl_context = None

        sock = await self.wrap_sock(sock, ssl_context, server_hostname)
        return AsyncHTTPConnection(
            target_url.raw[:3],
            ssl_context=ssl_context,
            socket=sock,
            backend=self.httpcore_backend,
        )

    async def start_tls(
        self,
        url: typing.Tuple[bytes, bytes, typing.Optional[int]],
        sock: SocketStream,
        ssl_context: typing.Optional[ssl.SSLContext],
        timeout: typing.Dict,
    ) -> AsyncHTTPConnection:
        # The asyncio start_tls method has a hardcoded check for this attribute failing with TypeError if this
        # attribute isn't True. The SSL transport used is compatible with Start TLS so setting this here bypasses
        # that problem. This is used when creating a TLS connection which proxies to a TLS endpoint.
        # https://github.com/python/cpython/blob/d9151cb45371836d39b6d53afb50c5bcd353c661/Lib/asyncio/base_events.py#L1210-L1212
        # https://github.com/encode/httpcore/issues/254
        stream_writer = sock.stream_writer
        if url[0] == b"https" and not hasattr(stream_writer.transport, "_start_tls_compatible"):
            setattr(stream_writer.transport, "_start_tls_compatible", True)

        tls_sock = await sock.start_tls(url[1], ssl_context, timeout)

        return AsyncHTTPConnection(
            url,
            ssl_context=ssl_context,
            socket=tls_sock,
            backend=self.httpcore_backend,
        )

    async def wrap_sock(
        self,
        sock: socket.socket,
        ssl_context: typing.Optional[ssl.SSLContext],
        server_hostname: typing.Optional[str],
    ) -> SocketStream:
        # httpcore backends do not provide a way to wrap an existing socket.
        if not ssl_context:
            server_hostname = None

        stream_reader, stream_writer = await asyncio.open_connection(
            sock=sock, ssl=ssl_context, server_hostname=server_hostname
        )
        return SocketStream(stream_reader=stream_reader, stream_writer=stream_writer)


class SyncBackend:
    def __init__(self):
        self.httpcore_backend = _SyncBackend()

    def new_connection(
        self,
        origin: typing.Tuple[bytes, bytes, typing.Optional[int]],
        sock: SyncSocketStream,
        ssl_context: typing.Optional[ssl.SSLContext],
    ) -> SyncHTTPConnection:
        return SyncHTTPConnection(
            origin,
            socket=sock,
            ssl_context=ssl_context,
            backend=self.httpcore_backend,
        )

    def new_socks_connection(
        self,
        proxy_url: httpx.URL,
        target_url: httpx.URL,
        timeout: typing.Dict,
        ssl_context: typing.Optional[ssl.SSLContext],
    ) -> SyncHTTPConnection:
        if not HAS_SOCKS:
            raise ImportError("Need pypsrp[socks] to be installed")

        # python-socks doesn't natively understand socks5h, we adjust the prefix and set rdns based on whether socks5h is
        # set or not.
        scheme = proxy_url.scheme
        proxy_url = str(proxy_url)
        rdns = False
        if scheme == "socks5h":
            rdns = True
            proxy_url = proxy_url.replace("socks5h://", "socks5://", 1)

        connect_timeout = timeout.get("connect", 60)
        proxy = SyncSocks.from_url(proxy_url, rdns=rdns)
        sock = proxy.connect(dest_host=target_url.host, dest_port=target_url.port, timeout=connect_timeout)

        if target_url.scheme == "https":
            server_hostname = target_url.host
        else:
            server_hostname = None
            ssl_context = None

        sock = self.wrap_sock(sock, ssl_context, server_hostname)
        return SyncHTTPConnection(
            target_url.raw[:3],
            ssl_context=ssl_context,
            socket=sock,
            backend=self.httpcore_backend,
        )

    def start_tls(
        self,
        url: typing.Tuple[bytes, bytes, typing.Optional[int]],
        sock: SyncSocketStream,
        ssl_context: typing.Optional[ssl.SSLContext],
        timeout: typing.Dict,
    ) -> SyncHTTPConnection:
        if url[0] == b"http":
            tls_sock = sock.start_tls(url[1], ssl_context, timeout)

        else:
            # The sync backend just calls ssl.SSLContext.wrap_socket() which fails if the existing socket is a TLS wrapped
            # socket. Instead we need to return a socket like object which does the BIO wrapping internally.
            tls_sock = SyncSocketStream(TLSSocket(sock.sock, ssl_context, url[1]))

        return SyncHTTPConnection(
            url,
            ssl_context=ssl_context,
            socket=tls_sock,
            backend=self.httpcore_backend,
        )

    def wrap_sock(
        self,
        sock: socket.socket,
        ssl_context: typing.Optional[ssl.SSLContext],
        server_hostname: typing.Optional[str],
    ) -> SyncSocketStream:
        # httpcore backends do not provide a way to wrap an existing socket.
        if ssl_context:
            sock = ssl_context.wrap_socket(sock, server_hostname=server_hostname)

        return SyncSocketStream(sock=sock)


# Mostly modeled after urllib3
# https://github.com/urllib3/urllib3/blob/main/src/urllib3/util/ssltransport.py
class TLSSocket:
    def __init__(
        self,
        sock: socket.socket,
        ssl_context: ssl.SSLContext,
        hostname: bytes,
    ):
        self._sock = sock
        self._in_bio = ssl.MemoryBIO()
        self._out_bio = ssl.MemoryBIO()
        self._ssl = ssl_context.wrap_bio(self._in_bio, self._out_bio, server_hostname=hostname)
        self._ssl_io_loop(self._ssl.do_handshake)

    def __getattr__(self, name: str) -> typing.Any:
        # Favour any overloads on the ssl object before falling back to the socket.
        if hasattr(self._ssl, name):
            return getattr(self._ssl, name)

        else:
            return getattr(self._sock, name)

    def recv(self, buflen: int) -> bytes:
        return self._wrap_ssl_read(buflen)

    def send(self, data: bytes) -> int:
        return self._ssl_io_loop(self._ssl.write, data)

    def getpeercert(self, binary_form: bool):
        return self._ssl.getpeercert(binary_form)

    def _wrap_ssl_read(
        self,
        len: int,
    ) -> typing.Union[int, bytes]:
        buffer = bytearray(len)
        try:
            length = self._ssl_io_loop(self._ssl.read, len, buffer)
            return bytes(buffer[:length])
        except ssl.SSLError as e:
            if e.errno == ssl.SSL_ERROR_EOF:
                return 0  # eof, return 0.
            else:
                raise

    def _ssl_io_loop(
        self,
        func: typing.Callable,
        *args,
        **kwargs,
    ) -> typing.Any:
        """Performs an I/O loop between incoming/outgoing and the socket."""
        should_loop = True
        ret = None

        while should_loop:
            errno = None
            try:
                ret = func(*args, **kwargs)
            except ssl.SSLError as e:
                if e.errno not in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
                    # WANT_READ, and WANT_WRITE are expected, others are not.
                    raise e
                errno = e.errno

            buf = self._out_bio.read()
            self._sock.sendall(buf)

            if errno is None:
                should_loop = False
            elif errno == ssl.SSL_ERROR_WANT_READ:
                buf = self._sock.recv(16384)
                if buf:
                    self._in_bio.write(buf)
                else:
                    self._in_bio.write_eof()

        return ret
