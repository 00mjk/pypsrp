# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import base64
import logging
import threading
import typing
import uuid
import xml.etree.ElementTree as ElementTree

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
from psrp.io.wsman import AsyncWSManConnection, WSManConnection
from psrp.protocol.winrs import WinRS
from psrp.protocol.wsman import (
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
        connection_uri: str,
        configuration_name="Microsoft.PowerShell",
        buffer_mode: OutputBufferingMode = OutputBufferingMode.none,
        idle_timeout: typing.Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__()

        self._connection_args = args
        self._connection_kwargs = kwargs
        self._connection_kwargs["connection_uri"] = connection_uri
        self._connection = WSManConnection(*self._connection_args, **self._connection_kwargs)

        self._runspace_table: typing.Dict[uuid.UUID, WinRS] = {}
        self._listener_tasks: typing.Dict[str, threading.Thread] = {}
        self._connection_uri = connection_uri
        self._buffer_mode = buffer_mode
        self._idle_timeout = idle_timeout
        self._configuration_name = f"http://schemas.microsoft.com/powershell/{configuration_name}"

    def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        if pipeline_id is not None:
            self.signal(pool, str(pipeline_id).upper(), signal_code=SignalCode.terminate)

            pipeline_task = self._listener_tasks.pop(f"{pool.runspace_pool_id}:{pipeline_id}")
            pipeline_task.join()

        else:
            winrs = self._runspace_table[pool.runspace_pool_id]
            winrs.close()
            resp = self._connection.send(winrs.data_to_send())
            winrs.receive_data(resp)

            # We don't get a RnuspacePool state change response on our receive listener so manually change the state.
            pool.state = RunspacePoolState.Closed

            # Wait for the listener task(s) to complete and remove the RunspacePool from our internal table.
            for task_id in list(self._listener_tasks.keys()):
                if task_id.startswith(f"{pool.runspace_pool_id}:"):
                    self._listener_tasks.pop(task_id).join()

            del self._runspace_table[pool.runspace_pool_id]

            # No more connections left, close the underlying connection.
            if not self._runspace_table:
                self._connection.close()

    def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        payload = self.next_payload(pool)
        winrs.command("", args=[base64.b64encode(payload.data).decode()], command_id=str(pipeline_id).upper())
        resp = self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

        self._create_listener(pool, pipeline_id)

    def create(
        self,
        pool: ClientRunspacePool,
    ):
        winrs = WinRS(
            WSMan(self._connection_uri),
            self._configuration_name,
            shell_id=str(pool.runspace_pool_id).upper(),
            input_streams="stdin pr",
            output_streams="stdout",
        )
        self._runspace_table[pool.runspace_pool_id] = winrs

        payload = self.next_payload(pool)

        open_content = ElementTree.Element("creationXml", xmlns="http://schemas.microsoft.com/powershell")
        open_content.text = base64.b64encode(payload.data).decode()
        options = OptionSet()
        options.add_option("protocolversion", pool.our_capability.protocolversion, {"MustComply": "true"})
        winrs.open(options, open_content)

        resp = self._connection.send(winrs.data_to_send())
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
        winrs.send(stream, payload.data, command_id=str(payload.pipeline_id).upper())
        resp = self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

        return True

    def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
        signal_code: SignalCode = SignalCode.ps_ctrl_c,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.signal(signal_code, str(pipeline_id).upper())
        resp = self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

    def disconnect(
        self,
        pool: ClientRunspacePool,
        buffer_mode: OutputBufferingMode = OutputBufferingMode.none,
        idle_timeout: typing.Optional[typing.Union[int, float]] = None,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]
        rsp = NAMESPACES["rsp"]

        disconnect = ElementTree.Element("{%s}Disconnect" % rsp)
        if buffer_mode != OutputBufferingMode.none:
            buffer_mode_str = "Block" if buffer_mode == OutputBufferingMode.block else "Drop"
            ElementTree.SubElement(disconnect, "{%s}BufferMode" % rsp).text = buffer_mode_str

        if idle_timeout:
            idle_str = f"PT{idle_timeout}S"
            ElementTree.SubElement(disconnect, "{%s}IdleTimeout" % rsp).text = idle_str

        winrs.wsman.disconnect(winrs.resource_uri, disconnect, selector_set=winrs.selector_set)
        resp = self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

    def reconnect(
        self,
        pool: ClientRunspacePool,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.wsman.reconnect(winrs.resource_uri, selector_set=winrs.selector_set)
        resp = self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

        self._create_listener(pool)

    def enumerate(self) -> typing.AsyncIterable[typing.Tuple[uuid.UUID, typing.List[str]]]:
        winrs = WinRS(WSMan(self._connection_uri))
        winrs.enumerate()
        resp = self._connection.send(winrs.data_to_send())
        shell_enumeration = winrs.receive_data(resp)

        for shell in shell_enumeration.shells:
            shell.enumerate("http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command", shell.selector_set)
            resp = self._connection.send(winrs.data_to_send())
            cmd_enumeration = winrs.receive_data(resp)

            self._runspace_table[uuid.UUID(shell.shell_id)] = shell

            yield uuid.UUID(shell.shell_id), cmd_enumeration.commands

    def connect(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        rsp = NAMESPACES["rsp"]
        connect = ElementTree.Element("{%s}Connect" % rsp)
        if pipeline_id:
            connect.attrib["CommandId"] = str(pipeline_id).upper()
            options = None

        else:
            payload = self.next_payload(pool)

            options = OptionSet()
            options.add_option("protocolversion", pool.our_capability.protocolversion, {"MustComply": "true"})

            open_content = ElementTree.SubElement(
                connect, "connectXml", xmlns="http://schemas.microsoft.com/powershell"
            )
            open_content.text = base64.b64encode(payload.data).decode()

        winrs = self._runspace_table[pool.runspace_pool_id]
        winrs.wsman.connect(winrs.resource_uri, connect, option_set=options, selector_set=winrs.selector_set)
        resp = self._connection.send(winrs.data_to_send())
        event = winrs.wsman.receive_data(resp)

        if not pipeline_id:
            response_xml = event.body.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES).text

            psrp_resp = PSRPPayload(base64.b64decode(response_xml), StreamType.default, None)
            pool.receive_data(psrp_resp)

        self._create_listener(pool, pipeline_id=pipeline_id)

    def _create_listener(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        started = threading.Event()
        task = threading.Thread(target=self._listen, args=(started, pool, pipeline_id))
        self._listener_tasks[f'{pool.runspace_pool_id}:{str(pipeline_id) or ""}'] = task
        task.start()
        started.wait()

    def _listen(
        self,
        started: threading.Event,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        with WSManConnection(*self._connection_args, **self._connection_kwargs) as conn:
            while True:
                winrs.receive("stdout", command_id=(str(pipeline_id).upper() if pipeline_id else None))

                resp = conn.send(winrs.data_to_send())
                # TODO: Will the ReceiveResponse block if not all the fragments have been sent?
                started.set()

                try:
                    event: ReceiveResponseEvent = winrs.receive_data(resp)

                except OperationTimedOut:
                    # Occurs when there has been no output after the OperationTimeout set, just repeat the request
                    continue

                except (OperationAborted, ServiceStreamDisconnected) as e:
                    # Received when the shell or pipeline has been closed
                    break

                for psrp_data in event.get_streams().get("stdout", []):
                    msg = PSRPPayload(psrp_data, StreamType.default, pipeline_id)
                    self.queue_response(pool.runspace_pool_id, msg)

                # If the command is done then we've got nothing left to do here.
                # TODO: do we need to surface the exit_code into the protocol.
                if event.command_state == CommandState.done:
                    break

            if pipeline_id is None:
                self.queue_response(pool.runspace_pool_id, None)


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
        connection_uri: str,
        configuration_name="Microsoft.PowerShell",
        buffer_mode: OutputBufferingMode = OutputBufferingMode.none,
        idle_timeout: typing.Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__()

        self._connection_args = args
        self._connection_kwargs = kwargs
        self._connection_kwargs["connection_uri"] = connection_uri
        self._connection = AsyncWSManConnection(*self._connection_args, **self._connection_kwargs)

        self._runspace_table: typing.Dict[uuid.UUID, WinRS] = {}
        self._listener_tasks: typing.Dict[str, asyncio.Task] = {}
        self._connection_uri = connection_uri
        self._buffer_mode = buffer_mode
        self._idle_timeout = idle_timeout
        self._configuration_name = f"http://schemas.microsoft.com/powershell/{configuration_name}"

    async def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        if pipeline_id is not None:
            await self.signal(pool, pipeline_id, signal_code=SignalCode.terminate)

            pipeline_task = self._listener_tasks.pop(f"{pool.runspace_pool_id}:{pipeline_id}")
            await pipeline_task

        else:
            winrs = self._runspace_table[pool.runspace_pool_id]
            winrs.close()
            resp = await self._connection.send(winrs.data_to_send())
            winrs.receive_data(resp)

            # We don't get a RnuspacePool state change response on our receive listener so manually change the state.
            pool.state = RunspacePoolState.Closed

            # Wait for the listener task(s) to complete and remove the RunspacePool from our internal table.
            listen_tasks = []
            for task_id in list(self._listener_tasks.keys()):
                if task_id.startswith(f"{pool.runspace_pool_id}:"):
                    listen_tasks.append(self._listener_tasks.pop(task_id))

            await asyncio.gather(*listen_tasks)
            del self._runspace_table[pool.runspace_pool_id]

            # No more connections left, close the underlying connection.
            if not self._runspace_table:
                await self._connection.close()

    async def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        payload = self.next_payload(pool)
        winrs.command("", args=[base64.b64encode(payload.data).decode()], command_id=str(pipeline_id))
        resp = await self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

        await self._create_listener(pool, pipeline_id)

    async def create(
        self,
        pool: ClientRunspacePool,
    ):
        winrs = WinRS(
            WSMan(self._connection_uri),
            self._configuration_name,
            shell_id=str(pool.runspace_pool_id).upper(),
            input_streams="stdin pr",
            output_streams="stdout",
        )
        self._runspace_table[pool.runspace_pool_id] = winrs

        payload = self.next_payload(pool)

        open_content = ElementTree.Element("creationXml", xmlns="http://schemas.microsoft.com/powershell")
        open_content.text = base64.b64encode(payload.data).decode()
        options = OptionSet()
        options.add_option("protocolversion", pool.our_capability.protocolversion, {"MustComply": "true"})
        winrs.open(options, open_content)

        resp = await self._connection.send(winrs.data_to_send())
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

        winrs = self._runspace_table[pool.runspace_pool_id]

        stream = "stdin" if payload.stream_type == StreamType.default else "pr"
        winrs.send(stream, payload.data, command_id=payload.pipeline_id)
        resp = await self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

        return True

    async def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
        signal_code: SignalCode = SignalCode.ps_ctrl_c,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.signal(signal_code, str(pipeline_id).upper())
        resp = await self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

    async def connect(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        rsp = NAMESPACES["rsp"]
        connect = ElementTree.Element("{%s}Connect" % rsp)
        if pipeline_id:
            connect.attrib["CommandId"] = str(pipeline_id).upper()
            options = None

        else:
            payload = self.next_payload(pool)

            options = OptionSet()
            options.add_option("protocolversion", pool.our_capability.protocolversion, {"MustComply": "true"})

            open_content = ElementTree.SubElement(
                connect, "connectXml", xmlns="http://schemas.microsoft.com/powershell"
            )
            open_content.text = base64.b64encode(payload.data).decode()

        winrs = self._runspace_table[pool.runspace_pool_id]
        winrs.wsman.connect(winrs.resource_uri, connect, option_set=options, selector_set=winrs.selector_set)
        resp = await self._connection.send(winrs.data_to_send())
        event = winrs.wsman.receive_data(resp)

        if not pipeline_id:
            response_xml = event.body.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES).text

            psrp_resp = PSRPPayload(base64.b64decode(response_xml), StreamType.default, None)
            pool.receive_data(psrp_resp)

        await self._create_listener(pool, pipeline_id=pipeline_id)

    async def disconnect(
        self,
        pool: ClientRunspacePool,
        buffer_mode: OutputBufferingMode = OutputBufferingMode.none,
        idle_timeout: typing.Optional[typing.Union[int, float]] = None,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]
        rsp = NAMESPACES["rsp"]

        disconnect = ElementTree.Element("{%s}Disconnect" % rsp)
        if buffer_mode != OutputBufferingMode.none:
            buffer_mode_str = "Block" if buffer_mode == OutputBufferingMode.block else "Drop"
            ElementTree.SubElement(disconnect, "{%s}BufferMode" % rsp).text = buffer_mode_str

        if idle_timeout:
            idle_str = f"PT{idle_timeout}S"
            ElementTree.SubElement(disconnect, "{%s}IdleTimeout" % rsp).text = idle_str

        winrs.wsman.disconnect(winrs.resource_uri, disconnect, selector_set=winrs.selector_set)
        resp = await self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

    async def reconnect(
        self,
        pool: ClientRunspacePool,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        winrs.wsman.reconnect(winrs.resource_uri, selector_set=winrs.selector_set)
        resp = await self._connection.send(winrs.data_to_send())
        winrs.receive_data(resp)

    async def enumerate(self) -> typing.AsyncIterable[typing.Tuple[uuid.UUID, typing.List[str]]]:
        winrs = WinRS(WSMan(self._connection_uri))
        winrs.enumerate()
        resp = await self._connection.send(winrs.data_to_send())
        shell_enumeration = winrs.receive_data(resp)

        for shell in shell_enumeration.shells:
            shell.enumerate("http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command", shell.selector_set)
            resp = await self._connection.send(winrs.data_to_send())
            cmd_enumeration = winrs.receive_data(resp)

            self._runspace_table[shell.shell_id] = shell

            yield shell.shell_id, cmd_enumeration.commands

    async def _create_listener(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        started = asyncio.Event()
        task = asyncio.create_task(self._listen(started, pool, pipeline_id))
        self._listener_tasks[f'{pool.runspace_pool_id}:{pipeline_id or ""}'] = task
        await started.wait()

    async def _listen(
        self,
        started: asyncio.Event,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ):
        winrs = self._runspace_table[pool.runspace_pool_id]

        async with AsyncWSManConnection(*self._connection_args, **self._connection_kwargs) as conn:
            while True:
                winrs.receive("stdout", command_id=str(pipeline_id).upper() if pipeline_id else None)

                resp = await conn.send(winrs.data_to_send())
                # TODO: Will the ReceiveResponse block if not all the fragments have been sent?
                started.set()

                try:
                    event: ReceiveResponseEvent = winrs.receive_data(resp)

                except OperationTimedOut:
                    # Occurs when there has been no output after the OperationTimeout set, just repeat the request
                    continue

                except (OperationAborted, ServiceStreamDisconnected) as e:
                    # Received when the shell or pipeline has been closed
                    break

                for psrp_data in event.get_streams().get("stdout", []):
                    msg = PSRPPayload(psrp_data, StreamType.default, pipeline_id)
                    await self.queue_response(pool.runspace_pool_id, msg)

                # If the command is done then we've got nothing left to do here.
                # TODO: do we need to surface the exit_code into the protocol.
                if event.command_state == CommandState.done:
                    break

            if pipeline_id is None:
                await self.queue_response(pool.runspace_pool_id, None)
