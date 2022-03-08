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

import psrpcore
from psrpcore import ClientRunspacePool, PSRPPayload, StreamType
from psrpcore.types import RunspacePoolState

from psrp._connection.connection import (
    AsyncConnection,
    AsyncEventCallable,
    ConnectionInfo,
    EnumerationPipelineResult,
    EnumerationRunspaceResult,
    OutputBufferingMode,
    SyncConnection,
    SyncEventCallable,
)
from psrp._exceptions import (
    OperationAborted,
    OperationTimedOut,
    ServiceStreamDisconnected,
    UnexpectedSelectors,
)
from psrp._io.wsman import AsyncWSManHTTP, WSManConnectionData, WSManHTTP
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

PS_RESOURCE_PREFIX = "http://schemas.microsoft.com/powershell"


class WSManInfo(ConnectionInfo):
    def __init__(
        self,
        connection_info: WSManConnectionData,
        configuration_name: str = "Microsoft.PowerShell",
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[int] = None,
    ) -> None:
        self.connection_info = connection_info
        self.configuration_name = configuration_name
        self.buffer_mode = buffer_mode
        self.idle_timeout = idle_timeout

        # Used by enumerate as it contains the required selectors.
        self._shell: t.Optional[WinRS] = None

    async def create_async(
        self,
        pool: ClientRunspacePool,
        callback: AsyncEventCallable,
    ) -> "AsyncConnection":
        return AsyncWSManConnection(pool, callback, self, self._new_winrs_shell(pool))

    async def enumerate_async(self) -> t.AsyncIterator["EnumerationRunspaceResult"]:
        connection = AsyncWSManHTTP(self.connection_info)
        wsman = WSMan(self.connection_info.connection_uri)

        enumerate_winrs(wsman)
        resp = await connection.post(wsman.data_to_send())
        shell_enumeration = wsman.receive_data(resp)

        shells = receive_winrs_enumeration(wsman, shell_enumeration)[0]
        for shell in shells:
            if not shell.resource_uri.startswith(f"{PS_RESOURCE_PREFIX}/"):
                continue

            enumerate_winrs(
                wsman,
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command",
                selector_filter=shell.selector_set,
            )
            resp = await connection.post(wsman.data_to_send())
            cmd_enumeration = wsman.receive_data(resp)
            pipelines = [
                EnumerationPipelineResult(pid=c.command_id, state=c.state)
                for c in receive_winrs_enumeration(wsman, cmd_enumeration)[1]
            ]

            shell_id = shell.shell_id
            config_name = shell.resource_uri[len(PS_RESOURCE_PREFIX) + 1 :]

            new_connection_info = WSManInfo(
                self.connection_info,
                configuration_name=config_name,
                # TODO: Get these values from rsp:IdletimeOut and rsp:BufferMode
                buffer_mode=self.buffer_mode,
                idle_timeout=self.idle_timeout,
            )
            new_connection_info._shell = shell
            yield EnumerationRunspaceResult(
                connection_info=new_connection_info,
                rpid=shell_id,
                state=shell.state,
                pipelines=pipelines,
            )

    def _new_winrs_shell(
        self,
        pool: ClientRunspacePool,
    ) -> WinRS:
        return self._shell or WinRS(
            WSMan(self.connection_info.connection_uri),
            f"{PS_RESOURCE_PREFIX}/{self.configuration_name}",
            shell_id=str(pool.runspace_pool_id).upper(),
            input_streams="stdin pr",
            output_streams="stdout",
        )


class WSManConnection(SyncConnection):
    def __init__(
        self,
        pool: ClientRunspacePool,
        callback: SyncEventCallable,
        info: WSManInfo,
        shell: WinRS,
    ) -> None:
        super().__init__(pool, callback)

        self._info = info
        self._connection = WSManHTTP(self._info.connection_info)

        self._listener_tasks: t.Dict[t.Optional[uuid.UUID], threading.Thread] = {}
        self._shell = shell

    def close(
        self,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        if pipeline_id is not None:
            self.signal(pipeline_id, signal_code=SignalCode.TERMINATE)

            pipeline_task = self._listener_tasks.pop(pipeline_id)
            pipeline_task.join()

        else:
            self._shell.close()
            resp = self._connection.post(self._shell.data_to_send())
            self._shell.receive_data(resp)

            # Ugly hack but WSMan does not send a RnuspacePool state change response on our receive listener so this
            # does it manually to align with the other connection types.
            pool = self.get_runspace_pool()
            pool.state = RunspacePoolState.Closed
            closed_event = psrpcore.PSRPEvent.create(
                psrpcore.types.PSRPMessageType.RunspacePoolState,
                psrpcore.types.RunspacePoolStateMsg(RunspaceState=pool.state),
                pool.runspace_pool_id,
            )
            self.process_response(closed_event)

            # Wait for the listener task(s) to complete and remove the RunspacePool from our internal table.
            for task_id in list(self._listener_tasks.keys()):
                self._listener_tasks.pop(task_id).join()

            self._connection.close()

    def command(
        self,
        pipeline_id: uuid.UUID,
    ) -> None:
        payload = t.cast(PSRPPayload, self.next_payload())
        self._shell.command("", args=[base64.b64encode(payload.data).decode()], command_id=pipeline_id)
        resp = self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        self._create_listener(pipeline_id)

    def create(self) -> None:
        pool = self.get_runspace_pool()
        payload = t.cast(PSRPPayload, self.next_payload())

        open_content = ElementTree.Element("creationXml", xmlns=PS_RESOURCE_PREFIX)
        open_content.text = base64.b64encode(payload.data).decode()
        options = OptionSet()
        options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})
        self._shell.open(options, open_content)

        resp = self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        self._create_listener()

    def send(
        self,
        buffer: bool = False,
    ) -> bool:
        payload = self.next_payload(buffer=buffer)
        if not payload:
            return False

        stream = "stdin" if payload.stream_type == StreamType.default else "pr"
        self._shell.send(stream, payload.data, command_id=payload.pipeline_id)
        resp = self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        return True

    def signal(
        self,
        pipeline_id: uuid.UUID,
        signal_code: SignalCode = SignalCode.PS_CRTL_C,
    ) -> None:
        self._shell.signal(signal_code, pipeline_id)
        resp = self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

    def connect(
        self,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        rsp = NAMESPACES["rsp"]
        connect = ElementTree.Element("{%s}Connect" % rsp)
        if pipeline_id:
            connect.attrib["CommandId"] = str(pipeline_id).upper()
            options = None

        else:
            pool = self.get_runspace_pool()
            payload = t.cast(PSRPPayload, self.next_payload())

            options = OptionSet()
            options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})

            open_content = ElementTree.SubElement(connect, "connectXml", xmlns=PS_RESOURCE_PREFIX)
            open_content.text = base64.b64encode(payload.data).decode()

        self._shell.wsman.connect(
            self._shell.resource_uri, connect, option_set=options, selector_set=self._shell.selector_set
        )
        resp = self._connection.post(self._shell.data_to_send())
        event = self._shell.wsman.receive_data(resp)

        if not pipeline_id:
            response_xml = t.cast(
                ElementTree.Element, event.body.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES)
            )

            psrp_resp = PSRPPayload(base64.b64decode(response_xml.text or ""), StreamType.default, None)
            pool.receive_data(psrp_resp)

        self._create_listener(pipeline_id=pipeline_id)

    def disconnect(
        self,
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[t.Union[int, float]] = None,
    ) -> None:
        rsp = NAMESPACES["rsp"]

        disconnect = ElementTree.Element("{%s}Disconnect" % rsp)
        if buffer_mode != OutputBufferingMode.NONE:
            buffer_mode_str = "Block" if buffer_mode == OutputBufferingMode.BLOCK else "Drop"
            ElementTree.SubElement(disconnect, "{%s}BufferMode" % rsp).text = buffer_mode_str

        if idle_timeout:
            idle_str = f"PT{idle_timeout}S"
            ElementTree.SubElement(disconnect, "{%s}IdleTimeout" % rsp).text = idle_str

        self._shell.wsman.disconnect(self._shell.resource_uri, disconnect, selector_set=self._shell.selector_set)
        resp = self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

    def reconnect(self) -> None:
        self._shell.wsman.reconnect(self._shell.resource_uri, selector_set=self._shell.selector_set)
        resp = self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        self._create_listener()

    def _create_listener(
        self,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        started = threading.Event()
        task = threading.Thread(target=self._listen, args=(started, pipeline_id))
        self._listener_tasks[pipeline_id] = task
        task.start()
        started.wait()

    def _listen(
        self,
        started: threading.Event,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        with WSManHTTP(self._info.connection_info) as conn:
            while True:
                self._shell.receive("stdout", command_id=pipeline_id)

                resp = conn.post(self._shell.data_to_send())
                # TODO: Will the ReceiveResponse block if not all the fragments have been sent?
                started.set()

                try:
                    event = t.cast(ReceiveResponseEvent, self._shell.receive_data(resp))

                except OperationTimedOut:
                    # Occurs when there has been no output after the OperationTimeout set, just repeat the request
                    continue

                except (OperationAborted, ServiceStreamDisconnected):
                    # Received when the shell or pipeline has been closed
                    break

                for psrp_data in event.streams.get("stdout", []):
                    msg = PSRPPayload(psrp_data, StreamType.default, pipeline_id)
                    self.process_response(msg)

                # If the command is done then we've got nothing left to do here.
                # TODO: do we need to surface the exit_code into the protocol.
                if event.command_state == CommandState.DONE:
                    break


class AsyncWSManConnection(AsyncConnection):
    """Async ConnectionInfo for WSMan.

    Async ConnectionInfo implementation for WSMan/WinRM. This is the
    traditional PSRP connection used on Windows before SSH became available.
    It uses a series of SOAP based messages sent over HTTP/HTTPS.

    Args:
        connection_uri: The WSMan URI to connect to.
    """

    def __init__(
        self,
        pool: ClientRunspacePool,
        callback: AsyncEventCallable,
        info: WSManInfo,
        shell: WinRS,
    ) -> None:
        super().__init__(pool, callback)

        self._info = info
        self._connection = AsyncWSManHTTP(info.connection_info)

        self._listener_tasks: t.Dict[t.Optional[uuid.UUID], asyncio.Task] = {}
        self._shell = shell

    async def close(
        self,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        if pipeline_id is not None:
            await self.signal(pipeline_id, signal_code=SignalCode.TERMINATE)

            pipeline_task = self._listener_tasks.pop(pipeline_id)
            await pipeline_task

        else:
            self._shell.close()
            resp = await self._connection.post(self._shell.data_to_send())
            self._shell.receive_data(resp)

            # Ugly hack but WSMan does not send a RnuspacePool state change response on our receive listener so this
            # does it manually to align with the other connection types.
            pool = self.get_runspace_pool()
            pool.state = RunspacePoolState.Closed
            closed_event = psrpcore.PSRPEvent.create(
                psrpcore.types.PSRPMessageType.RunspacePoolState,
                psrpcore.types.RunspacePoolStateMsg(RunspaceState=pool.state),
                pool.runspace_pool_id,
            )
            await self.process_response(closed_event)

            # Wait for the listener task(s) to complete and remove the RunspacePool from our internal table.
            await asyncio.gather(*self._listener_tasks.values())
            await self._connection.close()

    async def command(
        self,
        pipeline_id: uuid.UUID,
    ) -> None:
        payload = t.cast(PSRPPayload, self.next_payload())
        self._shell.command("", args=[base64.b64encode(payload.data).decode()], command_id=pipeline_id)
        resp = await self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        await self._create_listener(pipeline_id)

    async def create(self) -> None:
        pool = self.get_runspace_pool()
        payload = t.cast(PSRPPayload, self.next_payload())

        open_content = ElementTree.Element("creationXml", xmlns=PS_RESOURCE_PREFIX)
        open_content.text = base64.b64encode(payload.data).decode()
        options = OptionSet()
        options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})
        self._shell.open(options, open_content)

        resp = await self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        await self._create_listener()

    async def send(
        self,
        buffer: bool = False,
    ) -> bool:
        payload = self.next_payload(buffer=buffer)
        if not payload:
            return False

        await self._listener_send(payload, self._connection)
        return True

    async def _listener_send(
        self,
        payload: PSRPPayload,
        connection: AsyncWSManHTTP,
    ) -> None:
        stream = "stdin" if payload.stream_type == StreamType.default else "pr"
        self._shell.send(stream, payload.data, command_id=payload.pipeline_id)
        resp = await connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

    async def signal(
        self,
        pipeline_id: uuid.UUID,
        signal_code: SignalCode = SignalCode.PS_CRTL_C,
    ) -> None:
        self._shell.signal(signal_code, pipeline_id)
        resp = await self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

    async def connect(
        self,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        rsp = NAMESPACES["rsp"]
        connect = ElementTree.Element("{%s}Connect" % rsp)
        if pipeline_id:
            connect.attrib["CommandId"] = str(pipeline_id).upper()
            options = None

        else:
            pool = self.get_runspace_pool()
            payload = t.cast(PSRPPayload, self.next_payload())

            options = OptionSet()
            options.add_option("protocolversion", str(pool.our_capability.protocolversion), {"MustComply": "true"})

            open_content = ElementTree.SubElement(connect, "connectXml", xmlns=PS_RESOURCE_PREFIX)
            open_content.text = base64.b64encode(payload.data).decode()

        self._shell.wsman.connect(
            self._shell.resource_uri, connect, option_set=options, selector_set=self._shell.selector_set
        )
        resp = await self._connection.post(self._shell.data_to_send())
        event = self._shell.wsman.receive_data(resp)

        if not pipeline_id:
            response_xml = t.cast(
                ElementTree.Element, event.body.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES)
            )

            psrp_resp = PSRPPayload(base64.b64decode(response_xml.text or ""), StreamType.default, None)
            pool.receive_data(psrp_resp)

        await self._create_listener(pipeline_id=pipeline_id)

    async def disconnect(
        self,
        buffer_mode: OutputBufferingMode = OutputBufferingMode.NONE,
        idle_timeout: t.Optional[t.Union[int, float]] = None,
    ) -> None:
        rsp = NAMESPACES["rsp"]

        disconnect = ElementTree.Element("{%s}Disconnect" % rsp)
        if buffer_mode != OutputBufferingMode.NONE:
            buffer_mode_str = "Block" if buffer_mode == OutputBufferingMode.BLOCK else "Drop"
            ElementTree.SubElement(disconnect, "{%s}BufferMode" % rsp).text = buffer_mode_str

        if idle_timeout:
            idle_str = f"PT{idle_timeout}S"
            ElementTree.SubElement(disconnect, "{%s}IdleTimeout" % rsp).text = idle_str

        self._shell.wsman.disconnect(self._shell.resource_uri, disconnect, selector_set=self._shell.selector_set)
        resp = await self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)
        # FIXME: Check if I have to stop the listener

    async def reconnect(self) -> None:
        self._shell.wsman.reconnect(self._shell.resource_uri, selector_set=self._shell.selector_set)
        resp = await self._connection.post(self._shell.data_to_send())
        self._shell.receive_data(resp)

        await self._create_listener()

    async def _create_listener(
        self,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        started = asyncio.Event()
        task = asyncio.create_task(self._listen(started, pipeline_id))
        self._listener_tasks[pipeline_id] = task
        await started.wait()

    async def _listen(
        self,
        started: asyncio.Event,
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        async with AsyncWSManHTTP(self._info.connection_info) as conn:
            try:
                while True:
                    self._shell.receive("stdout", command_id=pipeline_id)
                    resp = await conn.post(self._shell.data_to_send(), data_sent=None if started.is_set() else started)

                    try:
                        event = t.cast(ReceiveResponseEvent, self._shell.receive_data(resp))

                    except OperationTimedOut:
                        # Occurs when there has been no output after the OperationTimeout set, just repeat the request
                        continue

                    except (OperationAborted, UnexpectedSelectors, ServiceStreamDisconnected):
                        # Received when the shell or pipeline has been closed
                        break

                    for psrp_data in event.streams.get("stdout", []):
                        msg = PSRPPayload(psrp_data, StreamType.default, pipeline_id)

                        payload: t.Optional[PSRPPayload] = None
                        data_available = await self.process_response(msg)
                        if data_available:
                            payload = self.next_payload()

                        if payload:
                            await self._listener_send(payload, self._connection)

                    # If the command is done then we've got nothing left to do here.
                    if event.command_state == CommandState.DONE:
                        break
            except Exception as e:
                log.exception("WSMan listener encountered unhandled exception")
                await self.process_response(e)
