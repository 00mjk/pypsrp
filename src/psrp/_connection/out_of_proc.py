# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import base64
import logging
import threading
import typing as t
import uuid
import xml.etree.ElementTree as ElementTree

from psrpcore import ClientRunspacePool, PSRPPayload, StreamType

from psrp._connection.connection_info import AsyncConnectionInfo, ConnectionInfo

log = logging.getLogger(__name__)

_EMPTY_UUID = uuid.UUID(int=0)


class OutOfProcInfo(ConnectionInfo):
    def __new__(
        cls,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> "OutOfProcInfo":
        if cls == OutOfProcInfo:
            raise TypeError(
                f"Type {cls.__qualname__} cannot be instantiated; it can be used only as a base class for "
                f"PSRP out of process connection implementations."
            )

        return super().__new__(cls)  # type: ignore[return-value] # This returns OutOfProcInfo

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__listen_task: t.Optional[threading.Thread] = None
        self.__wait_condition = threading.Condition()
        self.__wait_table: t.List[t.Tuple[str, t.Optional[uuid.UUID]]] = []
        self.__write_lock = threading.Lock()

    #####################
    # OutOfProc Methods #
    #####################

    def read(self) -> t.Optional[bytes]:
        """Get the response data.

        Called by the background thread to read any responses from the peer.
        This should block until data is available.

        Returns:
            bytes: The raw response from the peer.
        """
        raise NotImplementedError()

    def write(
        self,
        data: bytes,
    ) -> None:
        """Write data.

        Write a request to send to the peer.

        Args:
            data: The data to write.
        """
        raise NotImplementedError()

    def start(self) -> None:
        """Start the connection.

        Starts the connection to the peer so it is ready to read and write to.
        """
        raise NotImplementedError()

    def stop(self) -> None:
        """Stop the connection.

        Stops the connection to the peer once the Runspace Pool has been
        closed.
        """
        raise NotImplementedError()

    def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        with self.__wait_condition:
            with self.__write_lock:
                self.write(_ps_guid_packet("Close", ps_guid=pipeline_id))
            self._wait_ack("Close", pipeline_id)

        if not pipeline_id:
            self.stop()

            if self.__listen_task:
                self.__listen_task.join()
                self.__listen_task = None

    def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        with self.__wait_condition:
            with self.__write_lock:
                self.write(_ps_guid_packet("Command", ps_guid=pipeline_id))
            self._wait_ack("Command", pipeline_id)

        self.send(pool)

    def create(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        self.start()
        self.__listen_task = threading.Thread(target=self._listen, args=(pool,))
        self.__listen_task.start()

        self.send(pool)

    def send(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> bool:
        payload = self.next_payload(pool, buffer=buffer)
        if not payload:
            return False

        with self.__wait_condition:
            with self.__write_lock:
                self.write(_ps_data_packet(payload.data, stream_type=payload.stream_type, ps_guid=payload.pipeline_id))
            self._wait_ack("Data", payload.pipeline_id)

        return True

    def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        with self.__wait_condition:
            with self.__write_lock:
                self.write(_ps_guid_packet("Signal", ps_guid=pipeline_id))
            self._wait_ack("Signal", pipeline_id)

    def _wait_ack(
        self,
        action: str,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        key = (f"{action}Ack", pipeline_id)
        self.__wait_table.append(key)
        self.__wait_condition.wait_for(lambda: key not in self.__wait_table)

    def _listen(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        while True:
            data = self.read()
            if not data:
                break

            packet = ElementTree.fromstring(data)
            data = base64.b64decode(packet.text) if packet.text else None
            ps_guid: t.Optional[uuid.UUID] = uuid.UUID(packet.attrib["PSGuid"])
            if ps_guid == _EMPTY_UUID:
                ps_guid = None

            payload_data: t.Optional[PSRPPayload] = None
            if data:
                payload_data = PSRPPayload(data, StreamType.default, ps_guid)

            tag = packet.tag
            if tag == "Data":
                self.queue_response(pool.runspace_pool_id, payload_data)

            else:
                with self.__wait_condition:
                    self.__wait_table.remove((tag, ps_guid))
                    self.__wait_condition.notify_all()

        self.queue_response(pool.runspace_pool_id, None)


class AsyncOutOfProcInfo(AsyncConnectionInfo):
    def __new__(
        cls,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> "AsyncOutOfProcInfo":
        if cls == AsyncOutOfProcInfo:
            raise TypeError(
                f"Type {cls.__qualname__} cannot be instantiated; it can be used only as a base class for "
                f"PSRP out of process connection implementations."
            )

        return super().__new__(cls)  # type: ignore[return-value] # This returns AsyncOutOfProcInfo

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__listen_task: t.Optional[asyncio.Task] = None
        self.__wait_condition = asyncio.Condition()
        self.__wait_table: t.List[t.Tuple[str, t.Optional[uuid.UUID]]] = []
        self.__write_lock = asyncio.Lock()

    #####################
    # OutOfProc Methods #
    #####################

    async def read(self) -> t.Optional[bytes]:
        """Get the response data.

        Called by the background thread to read any responses from the peer.
        This should block until data is available.

        Returns:
            bytes: The raw response from the peer.
        """
        raise NotImplementedError()

    async def write(
        self,
        data: bytes,
    ) -> None:
        """Write data.

        Write a request to send to the peer.

        Args:
            data: The data to write.
        """
        raise NotImplementedError()

    async def start(self) -> None:
        """Start the connection.

        Starts the connection to the peer so it is ready to read and write to.
        """
        raise NotImplementedError()

    async def stop(self) -> None:
        """Stop the connection.

        Stops the connection to the peer once the Runspace Pool has been
        closed.
        """
        raise NotImplementedError()

    async def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        async with self.__wait_condition:
            async with self.__write_lock:
                await self.write(_ps_guid_packet("Close", ps_guid=pipeline_id))
            await self._wait_ack("Close", pipeline_id)

        if not pipeline_id:
            await self.stop()

            if self.__listen_task:
                await self.__listen_task
                self.__listen_task = None

    async def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        async with self.__wait_condition:
            async with self.__write_lock:
                await self.write(_ps_guid_packet("Command", ps_guid=pipeline_id))
            await self._wait_ack("Command", pipeline_id)

        await self.send(pool)

    async def create(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        await self.start()
        self.__listen_task = asyncio.create_task(self._listen(pool))

        await self.send(pool)

    async def send(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> bool:
        payload = self.next_payload(pool, buffer=buffer)
        if not payload:
            return False

        async with self.__wait_condition:
            await self.write(
                _ps_data_packet(payload.data, stream_type=payload.stream_type, ps_guid=payload.pipeline_id)
            )
            await self._wait_ack("Data", payload.pipeline_id)

        return True

    async def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        async with self.__wait_condition:
            async with self.__write_lock:
                await self.write(_ps_guid_packet("Signal", ps_guid=pipeline_id))
            await self._wait_ack("Signal", pipeline_id)

    async def _wait_ack(
        self,
        action: str,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        key = (f"{action}Ack", pipeline_id)
        self.__wait_table.append(key)
        await self.__wait_condition.wait_for(lambda: key not in self.__wait_table)

        if self.__listen_task and self.__listen_task.done():
            self.__listen_task.result()

    async def _listen(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        buffer = bytearray()

        try:
            while True:
                try:
                    end_idx = buffer.index(b"\n")
                except ValueError:
                    # Don't have enough data - wait for more to arrive.
                    read_data = await self.read()
                    if not read_data:
                        break

                    buffer += read_data
                    continue

                data = bytes(buffer[:end_idx])
                buffer = buffer[end_idx + 1 :]

                try:
                    packet = ElementTree.fromstring(data)
                except ElementTree.ParseError as e:
                    # Use what's remaining in the buffer as part of the error message
                    msg = data + b"\n" + bytes(buffer)
                    raise ValueError(f"Failed to parse response: {msg.decode()}") from e

                data = base64.b64decode(packet.text) if packet.text else b""
                ps_guid: t.Optional[uuid.UUID] = uuid.UUID(packet.attrib["PSGuid"])
                if ps_guid == _EMPTY_UUID:
                    ps_guid = None

                payload_data: t.Optional[PSRPPayload] = None
                if data:
                    payload_data = PSRPPayload(data, StreamType.default, ps_guid)

                tag = packet.tag
                if tag == "Data":
                    await self.queue_response(pool.runspace_pool_id, payload_data)

                else:
                    async with self.__wait_condition:
                        self.__wait_table.remove((tag, ps_guid))
                        self.__wait_condition.notify_all()

        finally:
            await self.queue_response(pool.runspace_pool_id, None)
            async with self.__wait_condition:
                self.__wait_table = []
                self.__wait_condition.notify_all()


def _ps_data_packet(
    data: bytes,
    stream_type: StreamType = StreamType.default,
    ps_guid: t.Optional[uuid.UUID] = None,
) -> bytes:
    """Data packet for PSRP fragments

    This creates a data packet that is used to encode PSRP fragments when
    sending to the server.

    Args:
        data: The PSRP fragments to encode.
        stream_type: The stream type to target, Default or PromptResponse.
        ps_guid: Set to `None` or a 0'd UUID to target the RunspacePool,
            otherwise this should be the pipeline UUID.

    Returns:
        bytes: The encoded data XML packet.
    """
    ps_guid = ps_guid or _EMPTY_UUID
    stream_name = b"Default" if stream_type == StreamType.default else b"PromptResponse"
    return b"<Data Stream='%s' PSGuid='%s'>%s</Data>\n" % (stream_name, str(ps_guid).encode(), base64.b64encode(data))


def _ps_guid_packet(
    element: str,
    ps_guid: t.Optional[uuid.UUID] = None,
) -> bytes:
    """Common PSGuid packet for PSRP message.

    This creates a PSGuid packet that is used to signal events and stages in
    the PSRP exchange. Unlike the data packet this does not contain any PSRP
    fragments.

    Args:
        element: The element type, can be DataAck, Command, CommandAck, Close,
            CloseAck, Signal, and SignalAck.
        ps_guid: Set to `None` or a 0'd UUID to target the RunspacePool,
            otherwise this should be the pipeline UUID.

    Returns:
        bytes: The encoded PSGuid packet.
    """
    ps_guid = ps_guid or _EMPTY_UUID
    return b"<%s PSGuid='%s' />\n" % (element.encode(), str(ps_guid).encode())
