import typing as t

import pytest

import psrp


@pytest.mark.skip()
async def test_open_runspace(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        a = ""
