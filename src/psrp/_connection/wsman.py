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
from tkinter import E

import psrpcore
from psrpcore import ClientRunspacePool, PSRPPayload, StreamType
from psrpcore.types import RunspacePoolState

from psrp._connection.connection_info import (
    AsyncConnectionInfo,
    ConnectionInfo,
    OutputBufferingMode,
)
from psrp._exceptions import (
    OperationAborted,
    OperationTimedOut,
    ServiceStreamDisconnected,
)
from psrp._io.wsman import AsyncWSManConnection, WSManConnection, WSManConnectionData
from psrp._winrs import WinRS, enumerate_winrs, receive_winrs_enumeration
from psrp._wsman import (
    NAMESPACES,
    CommandState,
    OptionSet,
    ReceiveResponseEvent,
    SignalCode,
    WSMan,
)

log = logging.getLogger(__name__)


class WSManInfo(ConnectionInfo):
    def __init__(
        self,
        connection_info: WSManConnectionData,
        configuration_name: str = "Microsoft.PowerShell",
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[int] = None,
    ) -> None:
        super().__init__()

        self._connection_info = connection_info
        self._connection = WSManConnection(connection_info)

        self._runspace_table: t.Dict[uuid.UUID, WinRS] = {}
        self._listener_tasks: t.Dict[str, threading.Thread] = {}
        self._connection_uri = connection_info.connection_uri
        self._buffer_mode = buffer_mode
        self._idle_timeout = idle_timeout
        self._configuration_name = f"http://schemas.microsoft.com/powershell/{configuration_name}"

    def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        if pipeline_id is not None:
            self.signal(pool, pipeline_id, signal_code=SignalCode.TERMINATE)

            pipeline_task = self._listener_tasks.pop(f"{pool.runspace_pool_id}:{pipeline_id}")
            pipeline_task.join()

        else:
            winrs = self._runspace_table[pool.runspace_pool_id]
            winrs.close()
            resp = self._connection.post(winrs.data_to_send())
            winrs.receive_data(resp)

            # Ugly hack but WSMan does not send a RnuspacePool state change response on our receive listener so this
            # does it manually to align with the other connection types.
            pool.state = RunspacePoolState.Closed
            closed_event = psrpcore.PSRPEvent.create(
                psrpcore.types.PSRPMessageType.RunspacePoolState,
                psrpcore.types.RunspacePoolStateMsg(RunspaceState=pool.state),
                pool.runspace_pool_id,
            )
            self.process_response(pool, closed_event)

            # Wait for the listener task(s) to complete and remove the RunspacePool from our internal table.
            for task_id in list(self._listener_tasks.keys()):
                if task_id.startswith(f"{pool.runspace_pool_id}:"):
                    self._listener_tasks.pop(task_id).join()

            self.process_response(pool, None)
            del self._runspace_table[pool.runspace_pool_id]

            # No more connections left, close the underlying connection.
            if not self._runspace_table:
                self._connection.close()

    def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        payload = t.cast(PSRPPayload, self.next_payload(pool))
        winrs.command("", args=[base64.b64encode(payload.data).decode()], command_id=pipeline_id)
        resp = self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

        self._create_listener(pool, pipeline_id)

    def create(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        winrs = WinRS(
            WSMan(self._connection_uri),
            self._configuration_name,
            shell_id=str(pool.runspace_pool_id).upper(),
            input_streams="stdin pr",
            output_streams="stdout",
        )
        self._runspace_table[pool.runspace_pool_id] = winrs

        payload = t.cast(PSRPPayload, self.next_payload(pool))

        open_content = ElementTree.Element("creationXml", xmlns="http://schemas.microsoft.com/powershell")
        open_content.text = base64.b64encode(payload.data).decode()
        options = OptionSet()
        options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})
        winrs.open(options, open_content)

        resp = self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

        self._create_listener(pool)

    def send(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> bool:
        payload = self.next_payload(pool, buffer=buffer)
        if not payload:
            return False

        winrs = self._runspace_table[pool.runspace_pool_id]

        stream = "stdin" if payload.stream_type == StreamType.default else "pr"
        winrs.send(stream, payload.data, command_id=payload.pipeline_id)
        resp = self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

        return True

    def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
        signal_code: SignalCode = SignalCode.PS_CRTL_C,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.signal(signal_code, pipeline_id)
        resp = self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

    def connect(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        rsp = NAMESPACES["rsp"]
        connect = ElementTree.Element("{%s}Connect" % rsp)
        if pipeline_id:
            connect.attrib["CommandId"] = str(pipeline_id).upper()
            options = None

        else:
            payload = t.cast(PSRPPayload, self.next_payload(pool))

            options = OptionSet()
            options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})

            open_content = ElementTree.SubElement(
                connect, "connectXml", xmlns="http://schemas.microsoft.com/powershell"
            )
            open_content.text = base64.b64encode(payload.data).decode()

        winrs = self._runspace_table[pool.runspace_pool_id]
        winrs.wsman.connect(winrs.resource_uri, connect, option_set=options, selector_set=winrs.selector_set)
        resp = self._connection.post(winrs.data_to_send())
        event = winrs.wsman.receive_data(resp)

        if not pipeline_id:
            response_xml = t.cast(
                ElementTree.Element, event.body.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES)
            )

            psrp_resp = PSRPPayload(base64.b64decode(response_xml.text or ""), StreamType.default, None)
            pool.receive_data(psrp_resp)

        self._create_listener(pool, pipeline_id=pipeline_id)

    def disconnect(
        self,
        pool: ClientRunspacePool,
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[t.Union[int, float]] = None,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]
        rsp = NAMESPACES["rsp"]

        disconnect = ElementTree.Element("{%s}Disconnect" % rsp)
        if buffer_mode != OutputBufferingMode.NONE:
            buffer_mode_str = "Block" if buffer_mode == OutputBufferingMode.BLOCK else "Drop"
            ElementTree.SubElement(disconnect, "{%s}BufferMode" % rsp).text = buffer_mode_str

        if idle_timeout:
            idle_str = f"PT{idle_timeout}S"
            ElementTree.SubElement(disconnect, "{%s}IdleTimeout" % rsp).text = idle_str

        winrs.wsman.disconnect(winrs.resource_uri, disconnect, selector_set=winrs.selector_set)
        resp = self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

    def reconnect(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.wsman.reconnect(winrs.resource_uri, selector_set=winrs.selector_set)
        resp = self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

        self._create_listener(pool)

    def enumerate(self) -> t.Iterator[t.Tuple[uuid.UUID, t.List[uuid.UUID]]]:
        wsman = WSMan(self._connection_uri)
        enumerate_winrs(wsman)
        resp = self._connection.post(wsman.data_to_send())
        shell_enumeration = wsman.receive_data(resp)

        shells = receive_winrs_enumeration(wsman, shell_enumeration)[0]
        for shell in shells:
            enumerate_winrs(
                wsman,
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command",
                selector_filter=shell.selector_set,
            )
            resp = self._connection.post(wsman.data_to_send())
            cmd_enumeration = wsman.receive_data(resp)
            commands = receive_winrs_enumeration(wsman, cmd_enumeration)[1]

            shell_id = shell.shell_id
            self._runspace_table[shell_id] = shell

            yield shell_id, commands

    def _create_listener(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        started = threading.Event()
        task = threading.Thread(target=self._listen, args=(started, pool, pipeline_id))
        self._listener_tasks[f'{pool.runspace_pool_id}:{str(pipeline_id) or ""}'] = task
        task.start()
        started.wait()

    def _listen(
        self,
        started: threading.Event,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        with WSManConnection(self._connection_info) as conn:
            while True:
                winrs.receive("stdout", command_id=pipeline_id)

                resp = conn.post(winrs.data_to_send())
                # TODO: Will the ReceiveResponse block if not all the fragments have been sent?
                started.set()

                try:
                    event = t.cast(ReceiveResponseEvent, winrs.receive_data(resp))

                except OperationTimedOut:
                    # Occurs when there has been no output after the OperationTimeout set, just repeat the request
                    continue

                except (OperationAborted, ServiceStreamDisconnected):
                    # Received when the shell or pipeline has been closed
                    break

                for psrp_data in event.streams.get("stdout", []):
                    msg = PSRPPayload(psrp_data, StreamType.default, pipeline_id)
                    self.process_response(pool, msg)

                # If the command is done then we've got nothing left to do here.
                # TODO: do we need to surface the exit_code into the protocol.
                if event.command_state == CommandState.DONE:
                    break


class AsyncWSManInfo(AsyncConnectionInfo):
    """Async ConnectionInfo for WSMan.

    Async ConnectionInfo implementation for WSMan/WinRM. This is the
    traditional PSRP connection used on Windows before SSH became available.
    It uses a series of SOAP based messages sent over HTTP/HTTPS.

    Args:
        connection_uri: The WSMan URI to connect to.
    """

    def __init__(
        self,
        connection_info: WSManConnectionData,
        configuration_name: str = "Microsoft.PowerShell",
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[int] = None,
    ) -> None:
        super().__init__()

        self._connection_info = connection_info
        self._connection = AsyncWSManConnection(connection_info)

        self._runspace_table: t.Dict[uuid.UUID, WinRS] = {}
        self._listener_tasks: t.Dict[str, asyncio.Task] = {}
        self._connection_uri = connection_info.connection_uri
        self._buffer_mode = buffer_mode
        self._idle_timeout = idle_timeout
        self._configuration_name = f"http://schemas.microsoft.com/powershell/{configuration_name}"

    async def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        if pipeline_id is not None:
            await self.signal(pool, pipeline_id, signal_code=SignalCode.TERMINATE)

            pipeline_task = self._listener_tasks.pop(f"{pool.runspace_pool_id}:{pipeline_id}")
            await pipeline_task

        else:
            winrs = self._runspace_table[pool.runspace_pool_id]
            winrs.close()
            resp = await self._connection.post(winrs.data_to_send())
            winrs.receive_data(resp)

            # Ugly hack but WSMan does not send a RnuspacePool state change response on our receive listener so this
            # does it manually to align with the other connection types.
            pool.state = RunspacePoolState.Closed
            closed_event = psrpcore.PSRPEvent.create(
                psrpcore.types.PSRPMessageType.RunspacePoolState,
                psrpcore.types.RunspacePoolStateMsg(RunspaceState=pool.state),
                pool.runspace_pool_id,
            )
            await self.process_response(pool, closed_event)

            # Wait for the listener task(s) to complete and remove the RunspacePool from our internal table.
            listen_tasks = []
            for task_id in list(self._listener_tasks.keys()):
                if task_id.startswith(f"{pool.runspace_pool_id}:"):
                    listen_tasks.append(self._listener_tasks.pop(task_id))

            await asyncio.gather(*listen_tasks)
            await self.process_response(pool, None)
            del self._runspace_table[pool.runspace_pool_id]

            # No more connections left, close the underlying connection.
            if not self._runspace_table:
                await self._connection.close()

    async def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        payload = t.cast(PSRPPayload, self.next_payload(pool))
        winrs.command("", args=[base64.b64encode(payload.data).decode()], command_id=pipeline_id)
        resp = await self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

        await self._create_listener(pool, pipeline_id)

    async def create(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        winrs = WinRS(
            WSMan(self._connection_uri),
            self._configuration_name,
            shell_id=str(pool.runspace_pool_id).upper(),
            input_streams="stdin pr",
            output_streams="stdout",
        )
        self._runspace_table[pool.runspace_pool_id] = winrs

        payload = t.cast(PSRPPayload, self.next_payload(pool))

        open_content = ElementTree.Element("creationXml", xmlns="http://schemas.microsoft.com/powershell")
        open_content.text = base64.b64encode(payload.data).decode()
        options = OptionSet()
        options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})
        winrs.open(options, open_content)

        resp = await self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

        await self._create_listener(pool)

    async def send(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> bool:
        payload = self.next_payload(pool, buffer=buffer)
        if not payload:
            return False

        await self._listener_send(pool, payload, self._connection)
        return True

    async def _listener_send(
        self,
        pool: ClientRunspacePool,
        payload: PSRPPayload,
        connection: AsyncWSManConnection,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        stream = "stdin" if payload.stream_type == StreamType.default else "pr"
        winrs.send(stream, payload.data, command_id=payload.pipeline_id)
        resp = await connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

    async def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
        signal_code: SignalCode = SignalCode.PS_CRTL_C,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.signal(signal_code, pipeline_id)
        resp = await self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

    async def connect(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        rsp = NAMESPACES["rsp"]
        connect = ElementTree.Element("{%s}Connect" % rsp)
        if pipeline_id:
            connect.attrib["CommandId"] = str(pipeline_id).upper()
            options = None

        else:
            payload = t.cast(PSRPPayload, self.next_payload(pool))

            options = OptionSet()
            options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})

            open_content = ElementTree.SubElement(
                connect, "connectXml", xmlns="http://schemas.microsoft.com/powershell"
            )
            open_content.text = base64.b64encode(payload.data).decode()

        winrs = self._runspace_table[pool.runspace_pool_id]
        winrs.wsman.connect(winrs.resource_uri, connect, option_set=options, selector_set=winrs.selector_set)
        resp = await self._connection.post(winrs.data_to_send())
        event = winrs.wsman.receive_data(resp)

        if not pipeline_id:
            response_xml = t.cast(
                ElementTree.Element, event.body.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES)
            )

            psrp_resp = PSRPPayload(base64.b64decode(response_xml.text or ""), StreamType.default, None)
            pool.receive_data(psrp_resp)

        await self._create_listener(pool, pipeline_id=pipeline_id)

    async def disconnect(
        self,
        pool: ClientRunspacePool,
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[t.Union[int, float]] = None,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]
        rsp = NAMESPACES["rsp"]

        disconnect = ElementTree.Element("{%s}Disconnect" % rsp)
        if buffer_mode != OutputBufferingMode.NONE:
            buffer_mode_str = "Block" if buffer_mode == OutputBufferingMode.BLOCK else "Drop"
            ElementTree.SubElement(disconnect, "{%s}BufferMode" % rsp).text = buffer_mode_str

        if idle_timeout:
            idle_str = f"PT{idle_timeout}S"
            ElementTree.SubElement(disconnect, "{%s}IdleTimeout" % rsp).text = idle_str

        winrs.wsman.disconnect(winrs.resource_uri, disconnect, selector_set=winrs.selector_set)
        resp = await self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

    async def reconnect(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.wsman.reconnect(winrs.resource_uri, selector_set=winrs.selector_set)
        resp = await self._connection.post(winrs.data_to_send())
        winrs.receive_data(resp)

    async def enumerate(self) -> t.AsyncIterator[t.Tuple[uuid.UUID, t.List[uuid.UUID]]]:
        wsman = WSMan(self._connection_uri)
        enumerate_winrs(wsman)
        resp = await self._connection.post(wsman.data_to_send())
        shell_enumeration = wsman.receive_data(resp)

        shells = receive_winrs_enumeration(wsman, shell_enumeration)[0]
        for shell in shells:
            enumerate_winrs(
                wsman,
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command",
                selector_filter=shell.selector_set,
            )
            resp = await self._connection.post(wsman.data_to_send())
            cmd_enumeration = wsman.receive_data(resp)
            commands = receive_winrs_enumeration(wsman, cmd_enumeration)[1]

            shell_id = shell.shell_id
            self._runspace_table[shell_id] = shell

            yield shell_id, commands

    async def _create_listener(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        started = asyncio.Event()
        task = asyncio.create_task(self._listen(started, pool, pipeline_id))
        self._listener_tasks[f'{pool.runspace_pool_id}:{pipeline_id or ""}'] = task
        await started.wait()

    async def _listen(
        self,
        started: asyncio.Event,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        winrs = self._runspace_table[pool.runspace_pool_id]

        async with AsyncWSManConnection(self._connection_info) as conn:
            while True:
                started.set()

                winrs.receive("stdout", command_id=pipeline_id)
                resp = await conn.post(winrs.data_to_send())

                try:
                    event = t.cast(ReceiveResponseEvent, winrs.receive_data(resp))

                except OperationTimedOut:
                    # Occurs when there has been no output after the OperationTimeout set, just repeat the request
                    continue

                except (OperationAborted, ServiceStreamDisconnected):
                    # Received when the shell or pipeline has been closed
                    break

                for psrp_data in event.streams.get("stdout", []):
                    msg = PSRPPayload(psrp_data, StreamType.default, pipeline_id)

                    payload: t.Optional[PSRPPayload] = None
                    data_available = await self.process_response(pool, msg)
                    if data_available:
                        payload = self.next_payload(pool)

                    if payload:
                        await self._listener_send(pool, payload, self._connection)

                # If the command is done then we've got nothing left to do here.
                if event.command_state == CommandState.DONE:
                    break
