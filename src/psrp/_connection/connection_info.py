# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import enum
import logging
import queue
import threading
import typing
import uuid

from psrpcore import ClientRunspacePool, PSRPEvent, PSRPPayload

log = logging.getLogger(__name__)


class OutputBufferingMode(enum.Enum):
    """Output Buffer Mode for disconnecting Runspaces.

    This is used to control what a disconnected PSRP session does when the
    output generated has exceeded the buffer capacity.
    """

    none = enum.auto()  #: No output buffer mode is selected, the mode is inherited from the session configuration.
    block = enum.auto()  #: When the output buffer is full, execution is suspended until the buffer is clear.
    drop = enum.auto()  #: When the output buffer is full, execution continues replacing older buffered output.


class _ConnectionInfoBase:
    def __new__(
        cls,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> "_ConnectionInfoBase":
        if cls in [_ConnectionInfoBase, ConnectionInfo, AsyncConnectionInfo]:
            raise TypeError(
                f"Type {cls.__qualname__} cannot be instantiated; it can be used only as a base class for "
                f"PSRP connection implementations."
            )

        return super().__new__(cls)

    def __init__(
        self,
    ) -> None:
        self.__buffer: typing.Dict[uuid.UUID, bytearray] = {}

    def get_fragment_size(
        self,
        pool: ClientRunspacePool,
    ) -> int:
        """Get the max PSRP fragment size.

        Gets the maximum size allowed for PSRP fragments in this Runspace Pool.

        Returns:
            int: The max fragment size.
        """
        return 32_768

    def next_payload(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> typing.Optional[PSRPPayload]:
        """Get the next payload.

        Get the next payload to exchange if there are any.

        Args:
            pool: The Runspace Pool to get the next payload for.
            buffer: Wait until the buffer as set by `self.fragment_size` has
                been reached before sending the payload.

        Returns:
            Optional[PSRPPayload]: The transport payload to send if there is
                one.
        """
        pool_buffer = self.__buffer.setdefault(pool.runspace_pool_id, bytearray())
        fragment_size = self.get_fragment_size(pool)
        psrp_payload = pool.data_to_send(fragment_size - len(pool_buffer))
        if not psrp_payload:
            return None

        pool_buffer += psrp_payload.data
        if buffer and len(pool_buffer) < fragment_size:
            return None

        log.debug("PSRP Send", pool_buffer)
        # No longer need the buffer for now
        del self.__buffer[pool.runspace_pool_id]
        return PSRPPayload(
            pool_buffer,
            psrp_payload.stream_type,
            psrp_payload.pipeline_id,
        )


class ConnectionInfo(_ConnectionInfoBase):
    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__data_queue: typing.Dict[uuid.UUID, queue.Queue[typing.Optional[PSRPPayload]]] = {}
        self.__queue_lock = threading.Lock()

    def queue_response(
        self,
        runspace_pool_id: uuid.UUID,
        data: typing.Optional[PSRPPayload] = None,
    ) -> None:
        """Queue received data.

        Queues the data received from the peer into the internal message queue
        for later processing. It is up to the implementing class to retrieve
        the data and queue it with this method.

        Args:
            runspace_pool_id: The Runspace Pool ID the data is associated with.
            data: The data to queue, can be set to `None` to indicate no more
                data is expected.
        """
        data_queue = self._get_pool_queue(runspace_pool_id)
        data_queue.put(data)

    def wait_event(
        self,
        pool: ClientRunspacePool,
    ) -> typing.Optional[PSRPEvent]:
        """Get the next PSRP event.

        Get the next PSRP event generated from the responses of the peer. It is
        up to the implementing class to retrieve the data and queue it so
        events can be generated.

        Args:
            pool: The Runspace Pool to get the next event for.

        Returns:
            Optional[PSRPEvent]: The PSRPEvent or `None` if the Runspace Pool
                has been closed with no more events expected.
        """
        while True:
            event = pool.next_event()
            if event:
                return event

            data_queue = self._get_pool_queue(pool.runspace_pool_id)
            msg = data_queue.get()
            if msg is None:
                return None

            log.debug("PSRP Receive", msg.data)
            pool.receive_data(msg)

    def _get_pool_queue(
        self,
        runspace_pool_id: uuid.UUID,
    ) -> queue.Queue[typing.Optional[PSRPPayload]]:
        with self.__queue_lock:
            self.__data_queue.setdefault(runspace_pool_id, queue.Queue())

        return self.__data_queue[runspace_pool_id]

    ################
    # PSRP Methods #
    ################

    def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ) -> None:
        """Close the Runspace Pool/Pipeline.

        Closes the Runspace Pool or Pipeline inside the Runspace Pool. This
            should also close the underlying connection if no more resources
            are being used.

        Args:
            pool: The Runspace Pool to close.
            pipeline_id: Closes this pipeline in the Runspace Pool.
        """
        raise NotImplementedError()

    def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        """Create the pipeline.

        Creates a pipeline in the Runspace Pool. This should send the first
        fragment of the
        :class:`CreatePipeline <psrp.dotnet.psrp_messages.CreatePipeline>` PSRP
        message.

        Args:
            pool: The Runspace Pool to create the pipeline in.
            pipeline_id: The Pipeline ID that needs to be created.
        """
        raise NotImplementedError()

    def create(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Create the Runspace Pool

        Creates the Runspace Pool specified. This should send only one fragment
        that contains at least the
        :class:`SessionCapability <psrp.dotnet.psrp_messages.SessionCapability>`
        PSRP message. The underlying connection should also be done if not
        already done so.

        Args:
            pool: The Runspace Pool to create.
        """
        raise NotImplementedError()

    def send_all(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Send all PSRP payloads.

        Send all PSRP payloads that are ready to send.

        Args:
            pool: The Runspace Pool to send all payloads to.
        """
        while True:
            sent = self.send(pool)
            if not sent:
                return

    def send(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> bool:
        """Send PSRP payload.

        Send the next PSRP payload for the Runspace Pool.

        Args:
            pool: The Runspace Pool to send the payload to.
            buffer: When set to `False` will always send the payload regardless
                of the size. When set to `True` will only send the payload if
                it hits the max fragment size.

        Returns:
            bool: Set to `True` if a payload was sent and `False` if there was
                no payloads for the pool to send.
        """
        raise NotImplementedError()

    def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ) -> None:
        """Send a signal to the Runspace Pool/Pipeline

        Sends a signal to the Runspace Pool or Pipeline. Currently PSRP only
        uses a signal to a Pipeline to ask the server to stop.

        Args:
            pool: The Runspace Pool that contains the pipeline to signal.
            pipeline_id: The pipeline to send the signal to.
        """
        raise NotImplementedError()

    #####################
    # Optional Features #
    #####################

    def connect(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ) -> None:
        """Connect to a Runspace Pool/Pipeline.

        Connects to a Runspace Pool or Pipeline that has been disconnected by
        another client. This is an optional feature that does not have to be
        implemented for the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to connect to.
            pipeline_id: If connecting to a pipeline, this is the pipeline id.
        """
        raise NotImplementedError()

    def disconnect(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Disconnect a Runspace Pool.

        Disconnects from a Runspace Pool so another client can connect to it.
        This is an optional feature that does not have to be implemented for
        the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to disconnect.
        """
        raise NotImplementedError()

    def reconnect(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Reconnect a Runspace Pool.

        Reconnect to a Runspace Pool that has been disconnected by the same
        client. This is an optional feature that does not have to be
        implemented for the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to disconnect.
        """
        raise NotImplementedError()

    def enumerate(self) -> typing.Iterator[typing.Tuple[str, typing.List[str]]]:
        """Find Runspace Pools or Pipelines.

        Find all the Runspace Pools or Pipelines on the connection. This is
        used to enumerate any disconnected Runspace Pools or Pipelines for
        `:meth:connect()` and `:meth:reconnect()`. This is an optional feature
        that does not have to be implemented for the core PSRP scenarios.

        Returns:
            Iterable[Tuple[str, List[str]]]: Will yield tuples that contains
                the Runspace Pool ID with a list of all the pipeline IDs for
                that Runspace Pool.
        """
        raise NotImplementedError()


class AsyncConnectionInfo(_ConnectionInfoBase):
    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__data_queue: typing.Dict[uuid.UUID, asyncio.Queue[typing.Optional[PSRPPayload]]] = {}
        self.__queue_lock = asyncio.Lock()

    async def queue_response(
        self,
        runspace_pool_id: uuid.UUID,
        data: typing.Optional[PSRPPayload] = None,
    ) -> None:
        data_queue = await self._get_pool_queue(runspace_pool_id)
        await data_queue.put(data)

    async def wait_event(
        self,
        pool: ClientRunspacePool,
    ) -> typing.Optional[PSRPEvent]:
        while True:
            event = pool.next_event()
            if event:
                return event

            data_queue = await self._get_pool_queue(pool.runspace_pool_id)
            msg = await data_queue.get()
            if msg is None:
                return None

            log.debug("PSRP Receive", msg.data)
            pool.receive_data(msg)

    async def _get_pool_queue(
        self,
        runspace_pool_id: uuid.UUID,
    ) -> asyncio.Queue[typing.Optional[PSRPPayload]]:
        async with self.__queue_lock:
            self.__data_queue.setdefault(runspace_pool_id, asyncio.Queue())

        return self.__data_queue[runspace_pool_id]

    ################
    # PSRP Methods #
    ################

    async def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ) -> None:
        """Close the Runspace Pool/Pipeline.

        Closes the Runspace Pool or Pipeline inside the Runspace Pool. This
            should also close the underlying connection if no more resources
            are being used.

        Args:
            pool: The Runspace Pool to close.
            pipeline_id: Closes this pipeline in the Runspace Pool.
        """
        raise NotImplementedError()

    async def command(
        self,
        pool: ClientRunspacePool,
        pipeline_id: uuid.UUID,
    ) -> None:
        """Create the pipeline.

        Creates a pipeline in the Runspace Pool. This should send the first
        fragment of the
        :class:`CreatePipeline <psrp.dotnet.psrp_messages.CreatePipeline>` PSRP
        message.

        Args:
            pool: The Runspace Pool to create the pipeline in.
            pipeline_id: The Pipeline ID that needs to be created.
        """
        raise NotImplementedError()

    async def create(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Create the Runspace Pool

        Creates the Runspace Pool specified. This should send only one fragment
        that contains at least the
        :class:`SessionCapability <psrp.dotnet.psrp_messages.SessionCapability>`
        PSRP message. The underlying connection should also be done if not
        already done so.

        Args:
            pool: The Runspace Pool to create.
        """
        raise NotImplementedError()

    async def send_all(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Send all PSRP payloads.

        Send all PSRP payloads that are ready to send.

        Args:
            pool: The Runspace Pool to send all payloads to.
        """
        while True:
            sent = await self.send(pool)
            if not sent:
                return

    async def send(
        self,
        pool: ClientRunspacePool,
        buffer: bool = False,
    ) -> bool:
        """Send PSRP payload.

        Send the next PSRP payload for the Runspace Pool.

        Args:
            pool: The Runspace Pool to send the payload to.
            buffer: When set to `False` will always send the payload regardless
                of the size. When set to `True` will only send the payload if
                it hits the max fragment size.

        Returns:
            bool: Set to `True` if a payload was sent and `False` if there was
                no payloads for the pool to send.
        """
        raise NotImplementedError()

    async def signal(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ) -> None:
        """Send a signal to the Runspace Pool/Pipeline

        Sends a signal to the Runspace Pool or Pipeline. Currently PSRP only
        uses a signal to a Pipeline to ask the server to stop.

        Args:
            pool: The Runspace Pool that contains the pipeline to signal.
            pipeline_id: The pipeline to send the signal to.
        """
        raise NotImplementedError()

    #####################
    # Optional Features #
    #####################

    async def connect(
        self,
        pool: ClientRunspacePool,
        pipeline_id: typing.Optional[uuid.UUID] = None,
    ) -> None:
        """Connect to a Runspace Pool/Pipeline.

        Connects to a Runspace Pool or Pipeline that has been disconnected by
        another client. This is an optional feature that does not have to be
        implemented for the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to connect to.
            pipeline_id: If connecting to a pipeline, this is the pipeline id.
        """
        raise NotImplementedError()

    async def disconnect(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Disconnect a Runspace Pool.

        Disconnects from a Runspace Pool so another client can connect to it.
        This is an optional feature that does not have to be implemented for
        the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to disconnect.
        """
        raise NotImplementedError()

    async def reconnect(
        self,
        pool: ClientRunspacePool,
    ) -> None:
        """Reconnect a Runspace Pool.

        Reconnect to a Runspace Pool that has been disconnected by the same
        client. This is an optional feature that does not have to be
        implemented for the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to disconnect.
        """
        raise NotImplementedError()

    async def enumerate(self) -> typing.AsyncIterator[typing.Tuple[str, typing.List[str]]]:
        """Find Runspace Pools or Pipelines.

        Find all the Runspace Pools or Pipelines on the connection. This is
        used to enumerate any disconnected Runspace Pools or Pipelines for
        `:meth:connect()` and `:meth:reconnect()`. This is an optional feature
        that does not have to be implemented for the core PSRP scenarios.

        Returns:
            Iterable[Tuple[str, List[str]]]: Will yield tuples that contains
                the Runspace Pool ID with a list of all the pipeline IDs for
                that Runspace Pool.
        """
        raise NotImplementedError()
