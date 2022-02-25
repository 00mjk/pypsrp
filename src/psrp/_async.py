# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import typing as t
import uuid

from psrpcore import (
    ClientGetCommandMetadata,
    ClientPowerShell,
    ClientRunspacePool,
    Command,
    DebugRecordEvent,
    ErrorRecordEvent,
    GetRunspaceAvailabilityEvent,
    InformationRecordEvent,
    MissingCipherError,
    PipelineHostCallEvent,
    PipelineOutputEvent,
    PipelineStateEvent,
    ProgressRecordEvent,
    PSRPEvent,
    RunspacePoolHostCallEvent,
    RunspacePoolStateEvent,
    SetRunspaceAvailabilityEvent,
    UserEventEvent,
    VerboseRecordEvent,
    WarningRecordEvent,
)
from psrpcore.types import (
    ApartmentState,
    CommandTypes,
    DebugRecord,
    ErrorCategoryInfo,
    ErrorRecord,
    InformationRecord,
    NETException,
    ProgressRecord,
    PSInvocationState,
    PSObject,
    PSRPMessageType,
    PSThreadOptions,
    RemoteStreamOptions,
    RunspacePoolState,
    VerboseRecord,
    WarningRecord,
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


class AsyncPSDataCollection(t.Generic[T2], t.List[T2]):
    def __init__(
        self,
        *args: t.Any,
        blocking_iterator: bool = False,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.completed = AsyncEvent[bool]()
        self.data_added = AsyncEvent[T2]()
        self.data_adding = AsyncEvent[T2]()
        self.blocking_iterator = blocking_iterator
        self._completed = False
        self._add_condition = asyncio.Condition()

    def __add__(
        self,
        value: t.List[T2],
    ) -> t.List[T2]:
        if self._completed:
            raise ValueError("FIXME: Better error when adding on completed stream")
        return super().__add__(value)

    def __aiter__(
        self,
    ) -> t.AsyncIterator[T2]:
        return self._aiter_next()

    def append(
        self,
        value: T2,
    ) -> None:
        if self._completed:
            raise ValueError("FIXME: Better error when appending on completed stream")
        return super().append(value)

    async def complete(self) -> None:
        self._completed = True
        await self.completed(True)

    def insert(
        self,
        index: t.SupportsIndex,
        value: T2,
    ) -> None:
        if self._completed:
            raise ValueError("FIXME: Better error when inserting on completed stream")
        return super().insert(index, value)

    async def _append(
        self,
        value: T2,
    ) -> None:
        await self.data_adding(value)

        async with self._add_condition:
            self.append(value)
            self._add_condition.notify_all()

        await self.data_added(value)

    async def _aiter_next(self) -> t.AsyncIterator[T2]:
        idx = 0
        while True:
            async with self._add_condition:
                if idx < len(self):
                    value = self[idx]
                    idx += 1
                    yield value

                elif self._completed or not self.blocking_iterator:
                    break

                else:
                    await self._add_condition.wait()


class AsyncPipelineTask:
    def __init__(
        self,
        completed: asyncio.Event,
        output_stream: t.Optional[AsyncPSDataCollection[t.Any]] = None,
    ) -> None:
        self._completed = completed
        self._output_stream = output_stream

    async def wait(self) -> t.Optional[AsyncPSDataCollection[t.Any]]:
        await self._completed.wait()
        return self._output_stream


class AsyncEvent(t.Generic[T2]):
    def __init__(self) -> None:
        self._callbacks: t.List[t.Callable[[T2], t.Awaitable[None]]] = []

    async def __call__(
        self,
        event: T2,
    ) -> None:
        for callback in self._callbacks:
            await callback(event)

    def __iadd__(
        self,
        value: t.Callable[[T2], t.Awaitable[None]],
    ) -> "AsyncEvent[T2]":
        self._callbacks.append(value)
        return self

    def __isub__(
        self,
        value: t.Callable[[T2], t.Awaitable[None]],
    ) -> "AsyncEvent[T2]":
        self._callbacks.remove(value)
        return self


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
        self.state_changed = AsyncEvent[RunspacePoolStateEvent]()
        self.user_event = AsyncEvent[UserEventEvent]()

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
        self.connection.register_pool_callback(self._pool.runspace_pool_id, self._event_received)

        self._ci_get_availability: t.Dict[int, GetRunspaceAvailabilityEvent] = {}
        self._ci_set_availability: t.Dict[int, SetRunspaceAvailabilityEvent] = {}
        self._event_conditions: t.Dict[PSRPMessageType, asyncio.Condition] = {
            mt: asyncio.Condition()
            for mt in [
                PSRPMessageType.ApplicationPrivateData,
                PSRPMessageType.EncryptedSessionKey,
                PSRPMessageType.RunspaceAvailability,
                PSRPMessageType.RunspacePoolInitData,
                PSRPMessageType.RunspacePoolState,
                PSRPMessageType.SessionCapability,
            ]
        }

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

    @property
    def application_private_data(self) -> t.Dict[str, t.Any]:
        return self._pool.application_private_data

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
            sess_condition = self._event_conditions[PSRPMessageType.SessionCapability]
            init_condition = self._event_conditions[PSRPMessageType.InitRunspacePool]
            data_condition = self._event_conditions[PSRPMessageType.ApplicationPrivateData]

            async with sess_condition, init_condition, data_condition:
                self._pool.connect()
                await self.connection.connect(self._pool)
                await sess_condition.wait()
                await init_condition.wait()
                await data_condition.wait()

            self._new_client = False

        else:
            await self.connection.reconnect(self._pool)

        self._pool.state = RunspacePoolState.Opened

    async def open(self) -> None:
        self._pool.open()

        condition = self._event_conditions[PSRPMessageType.RunspacePoolState]
        async with condition:
            await self.connection.create(self._pool)
            await condition.wait()

    async def close(self) -> None:
        if self.state != RunspacePoolState.Disconnected:
            condition = self._event_conditions[PSRPMessageType.RunspacePoolState]
            async with condition:
                tasks = [p.close() for p in self.pipeline_table.values()] + [self.connection.close(self._pool)]
                await asyncio.gather(*tasks)
                await condition.wait_for(lambda: self.state != RunspacePoolState.Opened)

    async def disconnect(self) -> None:
        self._pool.state = RunspacePoolState.Disconnecting
        await self.connection.disconnect(self._pool)
        self._pool.state = RunspacePoolState.Disconnected

        for pipeline in self.pipeline_table.values():
            pipeline.pipeline.state = PSInvocationState.Disconnected

    async def exchange_key(self) -> None:
        condition = self._event_conditions[PSRPMessageType.EncryptedSessionKey]
        async with condition:
            self._pool.exchange_key()
            await self.connection.send_all(self._pool)
            await condition.wait()

    async def reset_runspace_state(self) -> bool:
        ci = self._pool.reset_runspace_state()
        return await self._send_set_runspace_availability_ci(ci)

    async def set_max_runspaces(
        self,
        value: int,
    ) -> bool:
        ci = self._pool.set_max_runspaces(value)
        return await self._send_set_runspace_availability_ci(ci)

    async def set_min_runspaces(
        self,
        value: int,
    ) -> bool:
        ci = self._pool.set_min_runspaces(value)
        return await self._send_set_runspace_availability_ci(ci)

    async def get_available_runspaces(self) -> int:
        ci = self._pool.get_available_runspaces()

        condition = self._event_conditions[PSRPMessageType.RunspaceAvailability]
        async with condition:
            await self.connection.send_all(self._pool)
            await condition.wait_for(lambda: ci in self._ci_get_availability)

        return self._ci_get_availability.pop(ci).count

    async def _event_received(
        self,
        event: PSRPEvent,
    ) -> None:
        if event.pipeline_id:
            pipeline = self.pipeline_table[event.pipeline_id]
            await pipeline._event_received(event)
            return

        if isinstance(event, RunspacePoolStateEvent):
            await self.state_changed(event)

        elif isinstance(event, UserEventEvent):
            await self.user_event(event)

        elif isinstance(event, GetRunspaceAvailabilityEvent):
            self._ci_get_availability[event.ci] = event

        elif isinstance(event, SetRunspaceAvailabilityEvent):
            self._ci_set_availability[event.ci] = event

        elif isinstance(event, RunspacePoolHostCallEvent):
            raise NotImplementedError("Call PSHost method")

        condition = self._event_conditions.get(event.message_type, None)
        if condition:
            async with condition:
                condition.notify_all()

    async def _send_set_runspace_availability_ci(
        self,
        ci: t.Optional[int],
    ) -> bool:
        if ci is None:
            return True

        condition = self._event_conditions[PSRPMessageType.RunspaceAvailability]
        async with condition:
            await self.connection.send_all(self._pool)
            await condition.wait_for(lambda: ci in self._ci_set_availability)

        return self._ci_set_availability.pop(ci).success


class AsyncPipeline(t.Generic[T1]):
    def __init__(
        self,
        runspace_pool: AsyncRunspacePool,
        pipeline: T1,
    ) -> None:
        self.runspace_pool = runspace_pool
        self.pipeline: T1 = pipeline
        self.state_changed = AsyncEvent[PipelineStateEvent]()
        self.stream_debug = AsyncPSDataCollection[DebugRecord]()
        self.stream_error = AsyncPSDataCollection[ErrorRecord]()
        self.stream_information = AsyncPSDataCollection[InformationRecord]()
        self.stream_progress = AsyncPSDataCollection[ProgressRecord]()
        self.stream_verbose = AsyncPSDataCollection[VerboseRecord]()
        self.stream_warning = AsyncPSDataCollection[WarningRecord]()
        self._stream_output: t.Optional[AsyncPSDataCollection[t.Any]] = None

        self._event_conditions: t.Dict[PSRPMessageType, asyncio.Condition] = {
            mt: asyncio.Condition()
            for mt in [
                PSRPMessageType.PipelineState,
            ]
        }

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
        output_stream: t.Optional[AsyncPSDataCollection[t.Any]] = None,
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
        output_stream: t.Optional[AsyncPSDataCollection[t.Any]] = None,
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
        output_stream: t.Optional[AsyncPSDataCollection[t.Any]] = None,
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

    async def _event_received(
        self,
        event: PSRPEvent,
    ) -> None:
        if isinstance(event, PipelineStateEvent):
            # TODO: Need to close the pipeline
            await self.state_changed(event)

        elif isinstance(event, PipelineHostCallEvent):
            await self._on_host_call(event)

        elif isinstance(event, PipelineOutputEvent):
            raise NotImplementedError("Output stream")  # event.data

        elif isinstance(event, DebugRecordEvent):
            await self.stream_debug._append(event.record)

        elif isinstance(event, ErrorRecordEvent):
            await self.stream_error._append(event.record)

        elif isinstance(event, InformationRecordEvent):
            await self.stream_information._append(event.record)

        elif isinstance(event, ProgressRecordEvent):
            await self.stream_progress._append(event.record)

        elif isinstance(event, VerboseRecordEvent):
            await self.stream_verbose._append(event.record)

        elif isinstance(event, WarningRecordEvent):
            await self.stream_warning._append(event.record)

        condition = self._event_conditions.get(event.message_type, None)
        if condition:
            async with condition:
                condition.notify_all()

    def _new_task(
        self,
        output_stream: t.Optional[AsyncPSDataCollection[t.Any]] = None,
        completed: t.Optional[asyncio.Event] = None,
        for_stop: bool = False,
    ) -> AsyncPipelineTask:
        task_output = None
        if not output_stream:
            output_stream = task_output = AsyncPSDataCollection[t.Any]()
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
        event: PipelineHostCallEvent,
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
                await self.stream_error._append(error_record)
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
        output_stream: t.Optional[AsyncPSDataCollection[t.Any]] = None,
        completed: t.Optional[asyncio.Event] = None,
        buffer_input: bool = True,
    ) -> AsyncPipelineTask:
        self.pipeline.metadata.no_input = input_data is None

        return await super().invoke_async(input_data, output_stream, completed, buffer_input)
