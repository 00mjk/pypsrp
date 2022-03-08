# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import logging
import typing as t

import asyncssh
from psrpcore import ClientRunspacePool

from psrp._connection.connection import AsyncEventCallable, ConnectionInfo
from psrp._connection.out_of_proc import AsyncOutOfProcConnection

log = logging.getLogger(__name__)


class SSHInfo(ConnectionInfo):
    def __init__(
        self,
        hostname: str,
        port: int = 22,
        username: t.Optional[str] = None,
        password: t.Optional[str] = None,
        subsystem: str = "powershell",
        executable: t.Optional[str] = None,
        arguments: t.Optional[t.List[str]] = None,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.subsystem = subsystem
        self.executable = executable
        self.arguments = arguments or []

    async def create_async(
        self,
        pool: ClientRunspacePool,
        callback: AsyncEventCallable,
    ) -> "AsyncSSHInfo":
        return AsyncSSHInfo(pool, callback, self)


class _ClientSession(asyncssh.SSHClientSession):
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[t.Optional[bytes]] = asyncio.Queue()
        self._buffer = bytearray()

    def data_received(
        self,
        data: bytes,
        datatype: t.Optional[int],
    ) -> None:
        self.incoming.put_nowait(data)


class AsyncSSHInfo(AsyncOutOfProcConnection):
    def __init__(
        self,
        pool: ClientRunspacePool,
        callback: AsyncEventCallable,
        info: SSHInfo,
    ) -> None:
        super().__init__(pool, callback)

        self._info = info
        self._ssh: t.Optional[asyncssh.SSHClientConnection] = None
        self._channel: t.Optional[asyncssh.SSHClientChannel] = None
        self._session: t.Optional[_ClientSession] = None

    async def read(self) -> t.Optional[bytes]:
        if not self._session:
            raise Exception("FIXME: Session not started")

        return await self._session.incoming.get()

    async def write(
        self,
        data: bytes,
    ) -> None:
        if not self._channel:
            raise Exception("FIXME: Session not started")

        self._channel.write(data)

    async def start(self) -> None:
        conn_options = asyncssh.SSHClientConnectionOptions(
            known_hosts=None,
            username=self._info.username,
            password=self._info.password,
        )
        self._ssh = await asyncssh.connect(
            self._info.hostname,
            port=self._info.port,
            options=conn_options,
        )

        cmd: t.Union[str, t.Tuple[()]] = ()
        if self._info.executable:
            cmd = " ".join([self._info.executable] + self._info.arguments)
            subsystem = None

        else:
            subsystem = self._info.subsystem

        self._channel, self._session = await self._ssh.create_session(  # type: ignore[assignment]
            _ClientSession,
            command=cmd,
            subsystem=subsystem,
            encoding=None,
        )

    async def stop(self) -> None:
        if self._channel:
            self._channel.kill()
            self._channel = None

        if self._ssh:
            self._ssh.close()
            self._ssh = None

        if self._session:
            self._session.incoming.put_nowait(None)
