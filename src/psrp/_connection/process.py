# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import logging
import subprocess
import typing as t

from psrp._connection.out_of_proc import AsyncOutOfProcInfo, OutOfProcInfo

log = logging.getLogger(__name__)


class ProcessInfo(OutOfProcInfo):
    """ConnectionInfo for a Process.

    ConnectionInfo implementation for a native process. The data is read from
    the ``stdout`` pipe of the process and the input is read to the ``stdin``
    pipe. This can be used to create a Runspace Pool on a local PowerShell
    instance or any other process that can handle the raw PSRP OutOfProc
    messages.

    Args:
        executable: The executable to run, defaults to `pwsh`.
        arguments: A list of arguments to run, when the executable is `pwsh`
            then this defaults to `-NoProfile -NoLogo -s`.
    """

    def __init__(
        self,
        executable: str = "pwsh",
        arguments: t.Optional[t.List[str]] = None,
    ) -> None:
        super().__init__()

        self.executable = executable
        self.arguments = arguments or []
        if executable == "pwsh" and arguments is None:
            self.arguments = ["-NoProfile", "-NoLogo", "-s"]

        self._process: t.Optional[subprocess.Popen] = None

    def read(self) -> t.Optional[bytes]:
        if not self._process:
            raise Exception("FIXME: Process not started")

        return self._process.stdout.read(32_768) or None  # type: ignore[union-attr] # Will be set

    def write(
        self,
        data: bytes,
    ) -> None:
        if not self._process:
            raise Exception("FIXME: Process not started")

        writer: t.IO[t.Any] = self._process.stdin  # type: ignore[assignment] # Will be set
        writer.write(data)
        writer.flush()

    def start(self) -> None:
        pipe = subprocess.PIPE
        arguments = [self.executable]
        arguments.extend(self.arguments)

        self._process = subprocess.Popen(arguments, stdin=pipe, stdout=pipe, stderr=subprocess.STDOUT)

    def stop(self) -> None:
        if self._process and self._process.poll() is None:
            self._process.kill()
            self._process.wait()


class AsyncProcessInfo(AsyncOutOfProcInfo):
    """Async ConnectionInfo for a Process.

    Async ConnectionInfo implementation for a native process. The data is read
    from the ``stdout`` pipe of the process and the input is read to the
    ``stdin`` pipe. This can be used to create a Runspace Pool on a local
    PowerShell instance or any other process that can handle the raw PSRP
    OutOfProc messages.

    Args:
        executable: The executable to run, defaults to `pwsh`.
        arguments: A list of arguments to run, when the executable is `pwsh`
            then this defaults to `-NoProfile -NoLogo -s`.
    """

    def __init__(
        self,
        executable: str = "pwsh",
        arguments: t.Optional[t.List[str]] = None,
    ) -> None:
        super().__init__()

        self.executable = executable
        self.arguments = arguments or []
        if executable == "pwsh" and arguments is None:
            self.arguments = ["-NoProfile", "-NoLogo", "-s"]

        self._process: t.Optional[asyncio.subprocess.Process] = None

    async def read(self) -> t.Optional[bytes]:
        if not self._process:
            raise Exception("FIXME: Process not started")

        return await self._process.stdout.read(32_768) or None  # type: ignore[union-attr] # Will be set

    async def write(
        self,
        data: bytes,
    ) -> None:
        if not self._process:
            raise Exception("FIXME: Process not started")

        writer: asyncio.StreamWriter = self._process.stdin  # type: ignore[assignment] # Will be set
        writer.write(data)
        await writer.drain()

    async def start(self) -> None:
        pipe = subprocess.PIPE
        self._process = await asyncio.create_subprocess_exec(
            self.executable,
            *self.arguments,
            stdin=pipe,
            stdout=pipe,
            stderr=subprocess.STDOUT,
            limit=32_768,
        )

    async def stop(self) -> None:
        if self._process:
            self._process.kill()
            await self._process.wait()
            self._process = None
