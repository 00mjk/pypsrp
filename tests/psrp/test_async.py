import datetime
import typing as t

import psrpcore
import pytest
import pytest_mock

import psrp


async def test_open_runspace(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        assert rp.state == psrpcore.types.RunspacePoolState.Opened
        assert rp.max_runspaces == 1
        assert rp.min_runspaces == 1
        assert rp.pipeline_table == {}

    assert rp.state == psrpcore.types.RunspacePoolState.Closed


async def test_open_runspace_min_max(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc, min_runspaces=2, max_runspaces=3) as rp:
        assert rp.state == psrpcore.types.RunspacePoolState.Opened
        assert rp.max_runspaces == 3
        assert rp.min_runspaces == 2


async def test_open_runspace_invalid_min_max(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    with pytest.raises(
        ValueError, match="min_runspaces must be greater than 0 and max_runspaces must be greater than min_runspaces"
    ):
        async with psrp.AsyncRunspacePool(psrp_async_proc, min_runspaces=2, max_runspaces=1) as rp:
            pass


async def test_runspace_set_min_max(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        assert rp.min_runspaces == 1
        assert rp.max_runspaces == 1

        actual = await rp.get_available_runspaces()
        assert actual == 1

        # Will fail as max is lower than 1
        actual = await rp.set_min_runspaces(2)
        assert actual is False
        assert rp.min_runspaces == 1

        actual = await rp.set_max_runspaces(2)
        assert actual
        assert rp.max_runspaces == 2

        actual = await rp.set_min_runspaces(2)
        assert actual
        assert rp.min_runspaces == 2

        actual = await rp.set_min_runspaces(-1)
        assert actual is False
        assert rp.min_runspaces == 2

        actual = await rp.get_available_runspaces()
        assert actual == 2


@pytest.mark.skip
async def test_runspace_disconnect() -> None:
    raise NotImplementedError("Need WSMan connection")


async def test_runspace_application_arguments(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    app_args = {
        "test_var": "abcdef12345",
        "bool": True,
    }
    async with psrp.AsyncRunspacePool(psrp_async_proc, application_arguments=app_args) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.add_script("$PSSenderInfo.ApplicationArguments")

        actual = await ps.invoke()
        assert len(actual) == 1
        assert isinstance(actual[0], dict)
        assert actual[0] == app_args


async def test_runspace_reset_state(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.add_script("$global:TestVar = 'foo'")
        await ps.invoke()

        ps = psrp.AsyncPowerShell(rp)
        ps.add_script("$global:TestVar")
        actual = await ps.invoke()
        assert actual == ["foo"]

        actual = await rp.reset_runspace_state()
        assert actual

        actual = await ps.invoke()
        assert actual == [None]


async def test_runspace_host_call(
    psrp_async_proc: psrp.AsyncProcessInfo,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rp_host = psrp.PSHost(ui=psrp.PSHostUI())
    rp_write_line = mocker.MagicMock()
    monkeypatch.setattr(rp_host.ui, "read_line", lambda: "runspace line")
    monkeypatch.setattr(rp_host.ui, "write_line2", rp_write_line)

    ps_host = psrp.PSHost(ui=psrp.PSHostUI())
    ps_write_line = mocker.MagicMock()
    monkeypatch.setattr(ps_host.ui, "read_line", lambda: "pipeline line")
    monkeypatch.setattr(ps_host.ui, "write_line2", ps_write_line)

    async with psrp.AsyncRunspacePool(psrp_async_proc, host=rp_host) as rp:
        ps = psrp.AsyncPowerShell(rp, host=ps_host)
        ps.add_script(
            """
            $rs = [Runspace]::DefaultRunspace
            $rsHost = $rs.GetType().GetProperty("Host", 60).GetValue($rs)
            $rsHost.UI.ReadLine()
            $rsHost.UI.WriteLine("host output")
            """
        )
        actual = await ps.invoke()
        assert actual == ["runspace line"]
        rp_write_line.assert_called_once_with("host output")
        ps_write_line.assert_not_called()


async def test_runspace_host_call_failure(
    psrp_async_proc: psrp.AsyncProcessInfo, monkeypatch: pytest.MonkeyPatch
) -> None:
    rp_host = psrp.PSHost(ui=psrp.PSHostUI())

    async with psrp.AsyncRunspacePool(psrp_async_proc, host=rp_host) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.add_script(
            """
            $rs = [Runspace]::DefaultRunspace
            $rsHost = $rs.GetType().GetProperty("Host", 60).GetValue($rs)
            $rsHost.UI.WriteLine("host output")
            """
        )
        actual = await ps.invoke()
        assert actual == []
        assert ps.stream_error == []
        assert len(rp.stream_error) == 1
        assert isinstance(rp.stream_error[0], psrpcore.types.ErrorRecord)
        assert str(rp.stream_error[0]) == "NotImplementedError when running HostMethodIdentifier.WriteLine2"


async def test_runspace_user_event(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    class PSCallbacks:
        def __init__(self) -> None:
            self.events: t.List[psrpcore.PSRPEvent] = []

        async def __call__(self, event: psrpcore.PSRPEvent) -> None:
            self.events.append(event)

    callback = PSCallbacks()
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        rp.user_event += callback

        ps = psrp.AsyncPowerShell(rp)
        ps.state_changed += callback
        ps.add_script(
            """
            $null = $Host.Runspace.Events.SubscribeEvent(
                $null,
                "EventIdentifier",
                "EventIdentifier",
                $null,
                $null,
                $true,
                $true)
            $null = $Host.Runspace.Events.GenerateEvent(
                "EventIdentifier",
                "sender",
                @("my", "args"),
                "extra data")
            # Ensure the event comes before the script ends
            Start-Sleep -Milliseconds 500
            """
        )
        await ps.invoke()

        assert len(callback.events) == 2
        assert isinstance(callback.events[0], psrpcore.UserEventEvent)
        assert callback.events[0].event.EventIdentifier == 1
        assert callback.events[0].event.ComputerName is None
        assert callback.events[0].event.MessageData == "extra data"
        assert callback.events[0].event.Sender == "sender"
        assert callback.events[0].event.SourceArgs == ["my", "args"]
        assert callback.events[0].event.SourceIdentifier == "EventIdentifier"
        assert isinstance(callback.events[0].event.TimeGenerated, datetime.datetime)
        assert isinstance(callback.events[1], psrpcore.PipelineStateEvent)

        # Validate that it can remove the event and a user event is just lost in the ether
        rp.user_event -= callback
        ps.state_changed -= callback

        await ps.invoke()
        assert len(callback.events) == 2


async def test_run_powershell(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.add_script("echo 'hi'")
        actual = await ps.invoke()
        assert actual == ["hi"]


async def test_run_powershell_secure_string(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)

        secure_string = psrpcore.types.PSSecureString("my secret")
        ps.add_command("Write-Output").add_parameter("InputObject", secure_string)
        actual = await ps.invoke()
        assert len(actual) == 1
        assert isinstance(actual[0], psrpcore.types.PSSecureString)
        assert actual[0].decrypt() == "my secret"


async def test_run_powershell_receive_secure_string(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)

        ps.add_command("ConvertTo-SecureString").add_parameters(AsPlainText=True, Force=True, String="secret")
        actual = await ps.invoke()
        assert len(actual) == 1
        assert isinstance(actual[0], psrpcore.types.PSSecureString)

        with pytest.raises(
            psrpcore.MissingCipherError, match="Cannot \(de\)serialize a secure string without an exchanged session key"
        ):
            actual[0].decrypt()

        await rp.exchange_key()
        assert actual[0].decrypt() == "secret"


async def test_run_powershell_streams(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)

        ps.add_script(
            """
$DebugPreference = 'Continue'
$VerbosePreference = 'Continue'
$WarningPreference = 'Continue'

Write-Debug -Message debug
Write-Error -Message error
Write-Information -MessageData information
Write-Output -InputObject output
Write-Progress -Activity progress -Status done -PercentComplete 100
Write-Verbose -Message verbose
Write-Warning -Message warning
"""
        )

        actual = await ps.invoke()

        assert ps.had_errors  # An error record sets this
        assert actual == ["output"]

        assert len(ps.stream_debug) == 1
        assert ps.stream_debug[0].Message == "debug"

        assert len(ps.stream_error) == 1
        assert ps.stream_error[0].Exception.Message == "error"

        assert len(ps.stream_information) == 1
        assert ps.stream_information[0].MessageData == "information"

        assert len(ps.stream_progress) == 1
        assert ps.stream_progress[0].Activity == "progress"
        assert ps.stream_progress[0].PercentComplete == 100
        assert ps.stream_progress[0].StatusDescription == "done"

        assert len(ps.stream_verbose) == 1
        assert ps.stream_verbose[0].Message == "verbose"

        assert len(ps.stream_warning) == 1
        assert ps.stream_warning[0].Message == "warning"
