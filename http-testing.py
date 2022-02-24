import asyncio
import base64
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
    def __init__(self, httpcore_stream: typing.AsyncIterable[bytes]):
        self._httpcore_stream = httpcore_stream

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        async for part in self._httpcore_stream:
            yield part

    async def aclose(self) -> None:
        if hasattr(self._httpcore_stream, "aclose"):
            await self._httpcore_stream.aclose()


class AsyncWSManTransport(httpx.AsyncBaseTransport):
    def __init__(self, uri, ssl_context) -> None:
        url = httpx.URL(uri)
        self._auth_complete = False
        self._context = None
        self._hostname_override = url.host
        self._channel_bindings = None

        self._connection = httpcore.AsyncHTTPConnection(
            httpcore.Origin(url.raw_scheme, url.raw_host, url.port),
            ssl_context=ssl_context,
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
        out_token = None
        ext = request.extensions.copy()
        ext["trace"] = self.trace

        while True:
            headers = request.headers.raw
            if out_token:
                encoded_token = base64.b64encode(out_token)
                headers.append((b"Authorization", b"Negotiate " + encoded_token))
                out_token = None

            req = httpcore.Request(
                method=request.method,
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=headers,
                content=request.stream,
                extensions=ext,
            )

            resp = await self._connection.handle_async_request(req)

            if self._context:
                for name, value in resp.headers:
                    if name == b"WWW-Authenticate":
                        enc_in_token = value.split(b" ")[1]
                        in_token = base64.b64decode(enc_in_token)
                        out_token = self._context.step(in_token)
                        break

                if not out_token:
                    return httpx.Response(
                        status_code=resp.status,
                        headers=resp.headers,
                        stream=AsyncResponseStream(resp.stream),
                        extensions=resp.extensions,
                    )

                await resp.aread()
                await resp.aclose()

            else:
                return httpx.Response(
                    status_code=resp.status,
                    headers=resp.headers,
                    stream=AsyncResponseStream(resp.stream),
                    extensions=resp.extensions,
                )

    async def trace(
        self,
        event_name: str,
        info: typing.Dict[str, typing.Any],
    ) -> None:
        print(f"{event_name} - {info!s}")

        normalized_name = event_name.lower().replace(".", "_")
        event_handler = getattr(self, f"_{normalized_name}", None)
        if event_handler:
            await event_handler(info)

    async def _http11_send_request_headers_started(self, info: typing.Dict[str, typing.Any]) -> None:
        # The first request needs the context to be set up and the first token added as a header
        if self._context:
            return

        self._context = spnego.client(
            username=None,
            password=None,
            hostname=self._hostname_override,
            channel_bindings=self._channel_bindings,
            service="http",
            context_req=spnego.ContextReq.default,
            options=spnego.NegotiateOptions.none,
            protocol="negotiate",
        )
        token = self._context.step()
        encoded_token = base64.b64encode(token)
        info["request"].headers.append((b"Authorization", b"Negotiate " + encoded_token))

    async def _connection_start_tls_complete(self, info: typing.Dict[str, typing.Any]) -> None:
        # Once the TLS handshake is done we can immediately get the TLS channel bindings used later when creating the
        # auth context (as the headers have started).
        ssl_object = info["return_value"].get_extra_info("ssl_object")
        cert = ssl_object.getpeercert(True)
        cert_hash = get_tls_server_end_point_hash(cert)
        self._channel_bindings = spnego.channel_bindings.GssChannelBindings(
            application_data=b"tls-server-end-point:" + cert_hash
        )


async def connection(uri: str) -> None:
    headers = {
        "Accept-Encoding": "identity",
        "User-Agent": "Python PSRP Client",
    }
    ssl_context = httpx.create_ssl_context(verify=False)
    timeout = httpx.Timeout(30, connect=30, read=30)
    wsman = AsyncWSManTransport(uri, ssl_context)

    async with httpx.AsyncClient(headers=headers, timeout=timeout, transport=wsman) as client:
        resp = await client.post(uri)
        resp.raise_for_status()


asyncio.run(connection("https://server2019.domain.test:5986/wsman"))
