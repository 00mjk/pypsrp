# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import logging
import typing

from psrp._connection.out_of_proc import AsyncOutOfProcInfo

log = logging.getLogger(__name__)


HAS_SSH = True
try:
    import asyncssh
except ImportError:
    HAS_SSH = False

if HAS_SSH:

    class _ClientSession(asyncssh.SSHClientSession):
        def __init__(self):
            self.incoming: asyncio.Queue[typing.Optional[bytes]] = asyncio.Queue()
            self._buffer = bytearray()

        def data_received(self, data, datatype):
            self.incoming.put_nowait(data)

else:

    class _ClientSession:
        pass


class AsyncSSHInfo(AsyncOutOfProcInfo):
    def __init__(
        self,
        hostname: str,
        port: int = 22,
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        subsystem: str = "powershell",
        executable: typing.Optional[str] = None,
        arguments: typing.Optional[typing.List[str]] = None,
    ) -> None:
        if not HAS_SSH:
            raise Exception("FIXME asyncssh not installed")

        super().__init__()

        self._hostname = hostname
        self._port = port
        self._username = username
        self._password = password
        self._subsystem = subsystem
        self._executable = executable
        self._arguments = arguments or []

        self._ssh: typing.Optional[asyncssh.SSHClientConnection] = None
        self._channel: typing.Optional[asyncssh.SSHClientChannel] = None
        self._session: typing.Optional[_ClientSession] = None

    async def read(self) -> typing.Optional[bytes]:
        return await self._session.incoming.get()

    async def write(
        self,
        data: bytes,
    ) -> None:
        self._channel.write(data)

    async def start(self) -> None:
        conn_options = asyncssh.SSHClientConnectionOptions(
            known_hosts=None,
            username=self._username,
            password=self._password,
        )
        self._ssh = await asyncssh.connect(
            self._hostname,
            port=self._port,
            options=conn_options,
        )

        cmd = ()
        if self._executable:
            cmd = [self._executable]
            cmd.extend(self._arguments)
            cmd = " ".join(cmd)
            subsystem = None

        else:
            subsystem = self._subsystem

        self._channel, self._session = await self._ssh.create_session(
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

        self._session.incoming.put_nowait(None)
