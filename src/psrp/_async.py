# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import contextlib
import typing as t
import uuid

from psrpcore import (
    ClientGetCommandMetadata,
    ClientPowerShell,
    ClientRunspacePool,
    Command,
    GetRunspaceAvailabilityEvent,
    MissingCipherError,
    PipelineHostCallEvent,
    PSRPEvent,
    RunspacePoolHostCallEvent,
    SetRunspaceAvailabilityEvent,
)
from psrpcore.types import (
    ApartmentState,
    CommandTypes,
    ErrorCategoryInfo,
    ErrorRecord,
    NETException,
    PSInvocationState,
    PSObject,
    PSRPMessageType,
    PSThreadOptions,
    RemoteStreamOptions,
    RunspacePoolState,
)

from ._compat import iscoroutinefunction
from ._connection.connection_info import AsyncConnectionInfo
from ._host import PSHost, get_host_method

T1 = t.TypeVar("T1", bound=t.Union[ClientGetCommandMetadata, ClientPowerShell])
T2 = t.TypeVar("T2")


def _not_implemented() -> None:
    raise NotImplementedError()


async def _invoke_async(
    func: t.Callable[..., T2],
    *args: t.Any,
    **kwargs: t.Any,
) -> T2:
    if iscoroutinefunction(func):
        res = await func(*args, **kwargs)  # type: ignore[misc] # Not sure how else to document this
        return t.cast(T2, res)

    else:
        return func(*args, **kwargs)


class AsyncPSDataStream(list):
    """Collection for a PowerShell stream.

    This is a list of PowerShell objects for a PowerShell pipeline stream.
    This acts like a normal list but includes the `:meth:wait()` which can be
    used to asynchronously wait for any new objects to be added.
    """

    def __init__(
        self,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._added_idx: asyncio.Queue[t.Optional[int]] = asyncio.Queue()
        self._complete = False

    def __aiter__(self) -> "AsyncPSDataStream":
        return self

    async def __anext__(self) -> t.Any:
        val = await self.wait()
        if self._complete:
            raise StopAsyncIteration

        return val

    def append(
        self,
        value: t.Any,
    ) -> None:
        if not self._complete:
            super().append(value)
            self._added_idx.put_nowait(len(self) - 1)

    def finalize(self) -> None:
        if not self._complete:
            self._added_idx.put_nowait(None)

    async def wait(self) -> t.Optional[t.Any]:
        """Wait for a new entry.

        Waits for a new object to be added to the stream and returns that object once it is added.

        Returns:
            t.Optional[PSObject]: The PSObject added or `None`. If the queue has been finalized then `None` is
                also returned.
        """
        if self._complete:
            return None

        idx = await self._added_idx.get()
        if idx is None:
            self._complete = True
            return None

        return self[idx]


class AsyncPipelineTask:
    def __init__(
        self,
        completed: asyncio.Event,
        output_stream: t.Optional[AsyncPSDataStream] = None,
    ) -> None:
        self._completed = completed
        self._output_stream = output_stream

    async def wait(self) -> t.Optional[AsyncPSDataStream]:
        await self._completed.wait()
        return self._output_stream


class AsyncRunspacePool:
    def __init__(
        self,
        connection: AsyncConnectionInfo,
        apartment_state: ApartmentState = ApartmentState.Unknown,
        thread_options: PSThreadOptions = PSThreadOptions.Default,
        min_runspaces: int = 1,
        max_runspaces: int = 1,
        host: t.Optional[PSHost] = None,
        application_arguments: t.Optional[t.Dict] = None,
        runspace_pool_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        self.connection = connection
        self.host = host
        self.pipeline_table: t.Dict[uuid.UUID, AsyncPipeline] = {}

        self._new_client = False  # Used for reconnection as a new client.
        self._pool = ClientRunspacePool(
            apartment_state=apartment_state,
            host=host.get_host_info() if host else None,
            thread_options=thread_options,
            min_runspaces=min_runspaces,
            max_runspaces=max_runspaces,
            application_arguments=application_arguments,
            runspace_pool_id=runspace_pool_id,
        )
        self._ci_table = {}
        self._event_task = None
        self._registrations: t.Dict[
            PSRPMessageType, t.List[t.Union[asyncio.Condition, t.Callable[[PSRPEvent], None]]]
        ] = {mt: [asyncio.Condition()] for mt in PSRPMessageType}

    async def __aenter__(self) -> "AsyncRunspacePool":
        if self.state == RunspacePoolState.Disconnected:
            await self.connect()

        else:
            await self.open()

        return self

    async def __aexit__(
        self,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        await self.close()

    @property
    def max_runspaces(self) -> int:
        return self._pool.max_runspaces

    @property
    def min_runspaces(self) -> int:
        return self._pool.min_runspaces

    @property
    def state(self) -> RunspacePoolState:
        return self._pool.state

    @classmethod
    async def get_runspace_pools(
        cls,
        connection_info: AsyncConnectionInfo,
        host: t.Optional[PSHost] = None,
    ) -> t.AsyncIterator["AsyncRunspacePool"]:
        async for rpid, command_list in connection_info.enumerate():
            runspace_pool = AsyncRunspacePool(connection_info, host=host, runspace_pool_id=rpid)
            runspace_pool._pool.state = RunspacePoolState.Disconnected
            runspace_pool._new_client = True

            for cmd_id in command_list:
                ps = AsyncPowerShell(runspace_pool)
                ps.pipeline.pipeline_id = cmd_id
                ps.pipeline.state = PSInvocationState.Disconnected
                runspace_pool.pipeline_table[cmd_id] = ps

            yield runspace_pool

    def create_disconnected_power_shells(self) -> t.List["AsyncPipeline"]:
        return [p for p in self.pipeline_table.values() if p.pipeline.state == PSInvocationState.Disconnected]

    async def connect(self) -> None:
        if self._new_client:
            self._pool.connect()
            await self.connection.connect(self._pool)

            async with self._wait_condition(PSRPMessageType.SessionCapability) as sess, self._wait_condition(
                PSRPMessageType.RunspacePoolInitData
            ) as init, self._wait_condition(PSRPMessageType.ApplicationPrivateData) as data:

                self._event_task = asyncio.create_task(self._response_listener())
                await sess.wait()
                await init.wait()
                await data.wait()
            self._new_client = False

        else:
            await self.connection.reconnect(self._pool)
            self._event_task = asyncio.create_task(self._response_listener())

        self._pool.state = RunspacePoolState.Opened

    async def open(self) -> None:
        self._pool.open()
        await self.connection.create(self._pool)

        async with self._wait_condition(PSRPMessageType.RunspacePoolState) as cond:
            self._event_task = asyncio.create_task(self._response_listener())
            await cond.wait()

    async def close(self) -> None:
        if self.state != RunspacePoolState.Disconnected:
            tasks = [p.close() for p in self.pipeline_table.values()] + [self.connection.close(self._pool)]
            async with self._wait_condition(PSRPMessageType.RunspacePoolState) as cond:
                await asyncio.gather(*tasks)
                await cond.wait_for(lambda: self.state != RunspacePoolState.Opened)

        await asyncio.gather(self._event_task)

    async def disconnect(self) -> None:
        self._pool.state = RunspacePoolState.Disconnecting
        await self.connection.disconnect(self._pool)
        self._pool.state = RunspacePoolState.Disconnected

        for pipeline in self.pipeline_table.values():
            pipeline.pipeline.state = PSInvocationState.Disconnected

    async def exchange_key(self) -> None:
        self._pool.exchange_key()
        await self._send_and_wait_for(PSRPMessageType.EncryptedSessionKey)

    async def reset_runspace_state(self) -> bool:
        ci = self._pool.reset_runspace_state()
        return await self._validate_runspace_availability(ci)

    async def set_max_runspaces(
        self,
        value: int,
    ) -> bool:
        ci = self._pool.set_max_runspaces(value)
        return await self._validate_runspace_availability(ci)

    async def set_min_runspaces(
        self,
        value: int,
    ) -> bool:
        ci = self._pool.set_min_runspaces(value)
        return await self._validate_runspace_availability(ci)

    async def get_available_runspaces(self) -> int:
        ci = self._pool.get_available_runspaces()

        await self._send_and_wait_for(
            PSRPMessageType.RunspaceAvailability,
            lambda: ci in self._ci_table,
        )

        return self._ci_table.pop(ci).count

    def _get_event_registrations(
        self,
        event: PSRPEvent,
    ) -> t.List:
        if isinstance(
            event,
            (
                PipelineHostCallEvent,
                GetRunspaceAvailabilityEvent,
                SetRunspaceAvailabilityEvent,
                RunspacePoolHostCallEvent,
            ),
        ):
            self._ci_table[int(event.ci)] = event

        if event.pipeline_id:
            pipeline = self.pipeline_table[event.pipeline_id]
            reg_table = pipeline._registrations

        else:
            reg_table = self._registrations

        return reg_table[event.message_type]

    async def _response_listener(self) -> None:
        while True:
            event = await self.connection.wait_event(self._pool)
            if event is None:
                return

            registrations = self._get_event_registrations(event)
            for reg in registrations:
                if isinstance(reg, asyncio.Condition):
                    async with reg:
                        reg.notify_all()

                    continue

                try:
                    await _invoke_async(reg, event)

                except Exception as e:
                    # FIXM#E: log.warning this
                    print(f"Error running registered callback: {e!s}")

    async def _send_and_wait_for(
        self,
        message_type: PSRPMessageType,
        predicate: t.Optional[t.Callable] = None,
    ) -> None:
        async with self._wait_condition(message_type) as cond:
            await self.connection.send_all(self._pool)
            await (cond.wait_for(predicate) if predicate else cond.wait())

    async def _validate_runspace_availability(
        self,
        ci: t.Optional[int],
    ) -> bool:
        if ci is None:
            return True

        await self._send_and_wait_for(
            PSRPMessageType.RunspaceAvailability,
            lambda: ci in self._ci_table,
        )

        return self._ci_table.pop(ci).success

    @contextlib.asynccontextmanager
    async def _wait_condition(
        self,
        message_type: PSRPMessageType,
    ) -> t.AsyncIterable[asyncio.Condition]:
        cond = self._registrations[message_type][0]
        async with cond:
            yield cond


class AsyncPipeline(t.Generic[T1]):
    def __init__(
        self,
        runspace_pool: AsyncRunspacePool,
        pipeline: T1,
    ) -> None:
        self.runspace_pool = runspace_pool
        self.pipeline: T1 = pipeline
        self.streams: t.Dict[str, AsyncPSDataStream] = {}

        self._output_stream: t.Optional[AsyncPSDataStream] = None
        self._completed: t.Optional[asyncio.Event] = None
        self._completed_stop: t.Optional[asyncio.Event] = None
        self._host_tasks: t.Dict[int, t.Any] = {}
        self._close_lock = asyncio.Lock()

        self._registrations: t.Dict[
            PSRPMessageType, t.List[t.Union[asyncio.Condition, t.Callable[[PSRPEvent], None]]]
        ] = {mt: [asyncio.Condition()] for mt in PSRPMessageType}

        self._registrations[PSRPMessageType.PipelineState].append(self._on_state)
        self._registrations[PSRPMessageType.PipelineHostCall].append(self._on_host_call)
        self._registrations[PSRPMessageType.PipelineOutput].append(lambda e: self._output_stream.append(e.data))

        for name, mt in [
            ("debug", PSRPMessageType.DebugRecord),
            ("error", PSRPMessageType.ErrorRecord),
            ("information", PSRPMessageType.InformationRecord),
            ("progress", PSRPMessageType.ProgressRecord),
            ("verbose", PSRPMessageType.VerboseRecord),
            ("warning", PSRPMessageType.WarningRecord),
        ]:
            stream = AsyncPSDataStream()
            self.streams[name] = stream
            self._registrations[mt].append(lambda e: stream.append(e.record))

    @property
    def had_errors(self) -> bool:
        return self.state == PSInvocationState.Failed

    @property
    def state(self) -> PSInvocationState:
        return self.pipeline.state

    async def close(self) -> None:
        """Closes the pipeline.

        Closes the pipeline resource on the peer. This is done automatically when the pipeline is completed or the
        Runspace Pool is closed but can be called manually if desired.
        """
        # We call this from many places, we want a lock to ensure it's only run once.
        async with self._close_lock:
            pipeline = self.runspace_pool.pipeline_table.get(self.pipeline.pipeline_id)
            if not pipeline or pipeline.pipeline.state == PSInvocationState.Disconnected:
                return

            await self.runspace_pool.connection.close(self.runspace_pool._pool, self.pipeline.pipeline_id)
            del self.runspace_pool.pipeline_table[self.pipeline.pipeline_id]

    async def connect(self) -> t.AsyncIterable[PSObject]:
        task = await self.connect_async()
        return await task.wait()

    async def connect_async(
        self,
        output_stream: t.Optional[AsyncPSDataStream] = None,
        completed: t.Optional[asyncio.Event] = None,
    ) -> AsyncPipelineTask:
        task = self._new_task(output_stream, completed)

        await self.runspace_pool.connection.connect(self.runspace_pool._pool, self.pipeline.pipeline_id)
        self.runspace_pool.pipeline_table[self.pipeline.pipeline_id] = self
        self.runspace_pool._pool.pipeline_table[self.pipeline.pipeline_id] = self.pipeline
        self.pipeline.state = PSInvocationState.Running
        # TODO: Seems like we can't create a nested pipeline from a disconnected one.

        return task

    async def invoke(
        self,
        input_data: t.Optional[t.Union[t.Iterable, t.AsyncIterable]] = None,
        output_stream: t.Optional[AsyncPSDataStream] = None,
        buffer_input: bool = True,
    ) -> t.Optional[t.AsyncIterable[t.Optional[PSObject]]]:
        """Invoke the pipeline.

        Invokes the pipeline and yields the output as it is received. This takes the same arguments as
        `:meth:begin_invoke()` but instead of returning once the pipeline is started this will wait until it is
        complete.

        Returns:
            (t.AsyncIterable[PSObject]): An async iterable that can be iterated to receive the output objects as
                they are received.
        """
        output_task = await self.invoke_async(
            input_data=input_data,
            output_stream=output_stream,
            buffer_input=buffer_input,
        )
        return await output_task.wait()

    async def invoke_async(
        self,
        input_data: t.Optional[t.Union[t.Iterable, t.AsyncIterable]] = None,
        output_stream: t.Optional[AsyncPSDataStream] = None,
        completed: t.Optional[asyncio.Event] = None,
        buffer_input: bool = True,
    ) -> AsyncPipelineTask:
        """Begin the pipeline.

        Begin the pipeline execution and returns an async iterable that yields the output as they are received.

        Args:
            input_data: A list of objects to send as the input to the pipeline. Can be a normal or async iterable.
            output_stream:
            completed:
            buffer_input: Whether to buffer the input data and only send each object once the buffer is full (`True`)
                or individually as separate PSRP messages (`False`).

        Returns:
            (t.AsyncIterable[PSObject]): An async iterable that can be iterated to receive the output objects as
                they are received.
        """
        task = self._new_task(output_stream, completed)
        pool = self.runspace_pool._pool

        try:
            self.pipeline.start()
        except MissingCipherError:
            await self.runspace_pool.exchange_key()
            self.pipeline.start()

        self.runspace_pool.pipeline_table[self.pipeline.pipeline_id] = self
        await self.runspace_pool.connection.command(pool, self.pipeline.pipeline_id)
        await self.runspace_pool.connection.send_all(pool)

        if input_data is not None:
            if isinstance(input_data, t.Iterable):

                async def async_input_gen() -> t.AsyncIterator:
                    for data in input_data:
                        yield data

                input_gen = async_input_gen()
            else:
                input_gen = input_data

            async for data in input_gen:
                try:
                    self.pipeline.send(data)

                except MissingCipherError:
                    await self.runspace_pool.exchange_key()
                    self.pipeline.send(data)

                if buffer_input:
                    await self.runspace_pool.connection.send(pool, buffer=True)
                else:
                    await self.runspace_pool.connection.send_all(pool)

            self.pipeline.send_eof()
            await self.runspace_pool.connection.send_all(pool)

        return task

    async def stop(self) -> None:
        """Stops a running pipeline.

        Stops a running pipeline and waits for it to stop.
        """
        task = await self.stop_async()
        await task.wait()

    async def stop_async(
        self,
        completed: t.Optional[asyncio.Event] = None,
    ) -> AsyncPipelineTask:
        task = self._new_task(completed=completed, for_stop=True)
        await self.runspace_pool.connection.signal(self.runspace_pool._pool, self.pipeline.pipeline_id)

        return task

    async def _on_state(
        self,
        event: PSRPEvent,
    ) -> None:
        try:
            await self.close()

        finally:
            for stream in self.streams.values():
                stream.finalize()

            if self._output_stream:
                self._output_stream.finalize()
                self._output_stream = None

            if self._completed:
                self._completed.set()
                self._completed = None

            if self._completed_stop:
                self._completed_stop.set()
                self._completed_stop = None

    def _new_task(
        self,
        output_stream: t.Optional[AsyncPSDataStream] = None,
        completed: t.Optional[asyncio.Event] = None,
        for_stop: bool = False,
    ) -> AsyncPipelineTask:
        task_output = None
        if not output_stream:
            output_stream = task_output = AsyncPSDataStream()
        self._output_stream = output_stream

        completed = completed or asyncio.Event()
        if for_stop:
            self._completed_stop = completed

        else:
            self._completed = completed
            # TODO: Reset streams so we can append and iterate even more data

        return AsyncPipelineTask(completed, task_output)

    async def _on_host_call(
        self,
        event: PSRPEvent,
    ) -> None:
        host = getattr(self, "host", None) or self.runspace_pool.host

        ci = event.ci
        mi = event.method_identifier
        mp = event.method_parameters
        method_metadata = get_host_method(host, mi, mp)
        func = method_metadata.invoke

        error_record = None
        try:
            return_value = await _invoke_async(func or _not_implemented)

        except Exception as e:
            setattr(e, "mi", mi)

            # Any failure for non-void methods should be propagated back to the peer.
            e_msg = str(e)
            if not e_msg:
                e_msg = f"{type(e).__qualname__} when running {mi}"

            return_value = None
            error_record = ErrorRecord(
                Exception=NETException(e_msg),
                FullyQualifiedErrorId="RemoteHostExecutionException",
                CategoryInfo=ErrorCategoryInfo(
                    Reason="Exception",
                ),
            )

            if method_metadata.is_void:
                # PowerShell continues on even if the exception was on the client host
                self.streams["error"].append(error_record)
                return

        if not method_metadata.is_void:
            self.runspace_pool._pool.host_response(ci, return_value=return_value, error_record=error_record)
            await self.runspace_pool.connection.send_all(self.runspace_pool._pool)


class AsyncCommandMetaPipeline(AsyncPipeline[ClientGetCommandMetadata]):
    def __init__(
        self,
        runspace_pool: AsyncRunspacePool,
        name: t.Union[str, t.List[str]],
        command_type: CommandTypes = CommandTypes.All,
        namespace: t.Optional[t.List[str]] = None,
        arguments: t.Optional[t.List[str]] = None,
    ) -> None:
        pipeline = ClientGetCommandMetadata(
            runspace_pool=runspace_pool._pool,
            name=name,
            command_type=command_type,
            namespace=namespace,
            arguments=arguments,
        )
        super().__init__(runspace_pool, pipeline)


class AsyncPowerShell(AsyncPipeline[ClientPowerShell]):
    def __init__(
        self,
        runspace_pool: AsyncRunspacePool,
        add_to_history: bool = False,
        apartment_state: t.Optional[ApartmentState] = None,
        history: t.Optional[str] = None,
        host: t.Optional[PSHost] = None,
        is_nested: bool = False,
        remote_stream_options: RemoteStreamOptions = RemoteStreamOptions.none,
        redirect_shell_error_to_out: bool = True,
    ) -> None:
        pipeline = ClientPowerShell(
            runspace_pool=runspace_pool._pool,
            add_to_history=add_to_history,
            apartment_state=apartment_state,
            history=history,
            host=host.get_host_info() if host else None,
            is_nested=is_nested,
            remote_stream_options=remote_stream_options,
            redirect_shell_error_to_out=redirect_shell_error_to_out,
        )
        super().__init__(runspace_pool, pipeline)
        self.host = host

    def add_command(
        self,
        cmdlet: t.Union[str, Command],
        use_local_scope: t.Optional[bool] = None,
    ) -> "AsyncPowerShell":
        self.pipeline.add_command(cmdlet, use_local_scope)
        return self

    def add_script(
        self,
        script: str,
        use_local_scope: t.Optional[bool] = None,
    ) -> "AsyncPowerShell":
        self.pipeline.add_script(script, use_local_scope)
        return self

    def add_statement(self) -> "AsyncPowerShell":
        self.pipeline.add_statement()
        return self

    async def invoke_async(
        self,
        input_data: t.Optional[t.Union[t.Iterable, t.AsyncIterable]] = None,
        output_stream: t.Optional[AsyncPSDataStream] = None,
        completed: t.Optional[asyncio.Event] = None,
        buffer_input: bool = True,
    ) -> AsyncPipelineTask:
        self.pipeline.metadata.no_input = input_data is None

        return await super().invoke_async(input_data, output_stream, completed, buffer_input)
