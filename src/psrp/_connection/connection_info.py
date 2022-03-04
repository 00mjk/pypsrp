# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import logging
import typing as t
import uuid

from psrpcore import ClientRunspacePool, PSRPEvent, PSRPPayload

log = logging.getLogger(__name__)


class OutputBufferingMode(enum.Enum):
    """Output Buffer Mode for disconnecting Runspaces.

    This is used to control what a disconnected PSRP session does when the
    output generated has exceeded the buffer capacity.

    Attributes:
        NONE: No output buffer mode is selected, the mode is inherited from the
            session configuration.
        BLOCK: When the output buffer is full, execution is suspended until the
            buffer is clear.
        DROP: When the output buffer is full, execution continues replacing
            older buffered output.
    """

    NONE = enum.auto()
    BLOCK = enum.auto()
    DROP = enum.auto()


class _ConnectionInfoBase:
    def __new__(
        cls,
        *args: t.Any,
        **kwargs: t.Any,
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
        self.__buffer: t.Dict[uuid.UUID, bytearray] = {}

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
    ) -> t.Optional[PSRPPayload]:
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

        self.__event_callback: t.Dict[uuid.UUID, t.Callable[[PSRPEvent], bool]] = {}

    def register_pool_callback(
        self,
        runspace_pool_id: uuid.UUID,
        callback: t.Callable[[PSRPEvent], bool],
    ) -> None:
        """Register callback function for pool.

        Registers the callback function for a Runspace Pool that is called when
        a new PSRP event is available.

        Args:
            runspace_pool_id: The Runspace Pool identifier to register the
                callback on.
            callback: The function to invoke when a new event is available.
        """
        self.__event_callback[runspace_pool_id] = callback

    def process_response(
        self,
        pool: ClientRunspacePool,
        data: t.Optional[t.Union[PSRPEvent, PSRPPayload]] = None,
    ) -> bool:
        """Process an incoming PSRP payload.

        Processes any incoming PSRP payload received from the peer and invokes
        the pool callback function for any PSRP events inside that payload.
        Typically a PSRPPayload is the expected data type but a PSRPEvent can
        also be passed in for manual messages the connection wishes to send to
        the pool.

        Args:
            pool: The Runspace Pool the payload is for.
            data: The PSRPPayload or PSRPEvent to process. Will be None to
                signify no more data is expected for this pool.

        Returns:
            bool: More data has been added to be sent to the peer.
        """
        callback = self.__event_callback[pool.runspace_pool_id]

        if isinstance(data, PSRPEvent):
            return callback(data)

        if data:
            log.debug("PSRP Receive", data.data)
            pool.receive_data(data)

        else:
            del self.__event_callback[pool.runspace_pool_id]

        data_queued = False
        while True:
            event = pool.next_event()
            if not event:
                break

            res = callback(event)
            if res:
                data_queued = True

        return data_queued

    ################
    # PSRP Methods #
    ################

    def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
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
        pipeline_id: uuid.UUID,
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
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        """Connect to a Runspace Pool/Pipeline.

        Connects to a Runspace Pool or Pipeline that has been disconnected by
        another client. This is an optional feature that does not have to be
        implemented for the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to connect to.
            pipeline_id: If connecting to a pipeline, this is the pipeline id.
        """
        raise NotImplementedError("Disconnection operation not implemented on this connection type")

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
        raise NotImplementedError("Disconnection operation not implemented on this connection type")

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
        raise NotImplementedError("Disconnection operation not implemented on this connection type")

    def enumerate(self) -> t.Iterator[t.Tuple[uuid.UUID, t.List[uuid.UUID]]]:
        """Find Runspace Pools or Pipelines.

        Find all the Runspace Pools or Pipelines on the connection. This is
        used to enumerate any disconnected Runspace Pools or Pipelines for
        `:meth:connect()` and `:meth:reconnect()`. This is an optional feature
        that does not have to be implemented for the core PSRP scenarios.

        Returns:
            Iterator[Tuple[uuid.UUID, List[uuid.UUID]]]: Will yield tuples that
                contains the Runspace Pool ID with a list of all the pipeline
                IDs for that Runspace Pool.
        """
        raise NotImplementedError("Disconnection operation not implemented on this connection type")


class AsyncConnectionInfo(_ConnectionInfoBase):
    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__event_callback: t.Dict[uuid.UUID, t.Callable[[PSRPEvent], t.Awaitable[bool]]] = {}

    def register_pool_callback(
        self,
        runspace_pool_id: uuid.UUID,
        callback: t.Callable[[PSRPEvent], t.Awaitable[bool]],
    ) -> None:
        """Register callback coroutine for pool.

        Registers the callback coroutine for a Runspace Pool that is called
        when a new PSRP event is available.

        Args:
            runspace_pool_id: The Runspace Pool identifier to register the
                callback on.
            callback: The coroutine to invoke when a new event is available.
        """
        self.__event_callback[runspace_pool_id] = callback

    async def process_response(
        self,
        pool: ClientRunspacePool,
        data: t.Optional[t.Union[PSRPEvent, PSRPPayload]] = None,
    ) -> bool:
        """Process an incoming PSRP payload.

        Processes any incoming PSRP payload received from the peer and invokes
        the pool callback coroutine for any PSRP events inside that payload.
        Typically a PSRPPayload is the expected data type but a PSRPEvent can
        also be passed in for manual messages the connection wishes to send to
        the pool.

        Args:
            pool: The Runspace Pool the payload is for.
            data: The PSRPPayload or PSRPEvent to process. Will be None to
                signify no more data is expected for this pool.

        Returns:
            bool: More data has been added to be sent to the peer.
        """
        callback = self.__event_callback[pool.runspace_pool_id]

        if isinstance(data, PSRPEvent):
            return await callback(data)

        if data:
            log.debug("PSRP Receive", data.data)
            pool.receive_data(data)

        else:
            del self.__event_callback[pool.runspace_pool_id]

        data_queued = False
        while True:
            event = pool.next_event()
            if not event:
                break

            res = await callback(event)
            if res:
                data_queued = True

        return data_queued

    ################
    # PSRP Methods #
    ################

    async def close(
        self,
        pool: ClientRunspacePool,
        pipeline_id: t.Optional[uuid.UUID] = None,
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
        pipeline_id: uuid.UUID,
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
        pipeline_id: t.Optional[uuid.UUID] = None,
    ) -> None:
        """Connect to a Runspace Pool/Pipeline.

        Connects to a Runspace Pool or Pipeline that has been disconnected by
        another client. This is an optional feature that does not have to be
        implemented for the core PSRP scenarios.

        Args:
            pool: The Runspace Pool to connect to.
            pipeline_id: If connecting to a pipeline, this is the pipeline id.
        """
        raise NotImplementedError("Disconnection operation not implemented on this connection type")

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
        raise NotImplementedError("Disconnection operation not implemented on this connection type")

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
        raise NotImplementedError("Disconnection operation not implemented on this connection type")

    async def enumerate(self) -> t.AsyncIterator[t.Tuple[uuid.UUID, t.List[uuid.UUID]]]:
        """Find Runspace Pools or Pipelines.

        Find all the Runspace Pools or Pipelines on the connection. This is
        used to enumerate any disconnected Runspace Pools or Pipelines for
        `:meth:connect()` and `:meth:reconnect()`. This is an optional feature
        that does not have to be implemented for the core PSRP scenarios.

        Returns:
            AsyncIterator[Tuple[uuid.UUID, List[uuid.UUID]]]: Will yield tuples
                that contains the Runspace Pool ID with a list of all the
                pipeline IDs for that Runspace Pool.
        """
        raise NotImplementedError("Disconnection operation not implemented on this connection type")
        yield  # type: ignore[unreachable]  # The yield is needed for mypy to see this as an Iterator
