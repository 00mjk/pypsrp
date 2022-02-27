import os
import typing as t

import pytest

import psrp


def which(program: str) -> t.Optional[str]:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        exe = os.path.join(path, program)
        if os.path.isfile(exe) and os.access(exe, os.X_OK):
            return exe

    return None


PWSH_PATH = which("pwsh.exe" if os.name == "nt" else "pwsh")


@pytest.fixture(scope="function")
async def psrp_async_proc() -> t.AsyncIterator[psrp.AsyncProcessInfo]:
    if not PWSH_PATH:
        pytest.skip("Integration test requires pwsh")

    yield psrp.AsyncProcessInfo(executable=PWSH_PATH)
