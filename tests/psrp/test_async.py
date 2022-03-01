import datetime
import typing as t

import psrpcore
import pytest
import pytest_mock

import psrp


class PSEventCallbacks:
    def __init__(self) -> None:
        self.events: t.List[psrpcore.PSRPEvent] = []

    async def __call__(self, event: psrpcore.PSRPEvent) -> None:
        self.events.append(event)


class PSDataCallbacks:
    def __init__(self) -> None:
        self.data: t.List[t.Any] = []

    async def __call__(self, data: t.Any) -> None:
        self.data.append(data)


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

        actual_res = await rp.reset_runspace_state()
        assert actual_res

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
    callback = PSEventCallbacks()
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


async def test_powershell_secure_string(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)

        secure_string = psrpcore.types.PSSecureString("my secret")
        ps.add_command("Write-Output").add_parameter("InputObject", secure_string)
        actual = await ps.invoke()
        assert len(actual) == 1
        assert isinstance(actual[0], psrpcore.types.PSSecureString)
        assert actual[0].decrypt() == "my secret"


async def test_powershell_receive_secure_string(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
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


async def test_powershell_streams(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
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


async def test_powershell_state_changed(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    callbacks = PSEventCallbacks()

    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.state_changed += callbacks

        ps.add_script('echo "hi"')
        await ps.invoke()
        assert len(callbacks.events) == 1
        assert isinstance(callbacks.events[0], psrpcore.PipelineStateEvent)
        assert callbacks.events[0].state == ps.state

        ps.state_changed -= callbacks

        await ps.invoke()
        assert len(callbacks.events)


async def test_powershell_stream_events(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    callbacks = PSDataCallbacks()
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.add_script('$VerbosePreference = "Continue"; Write-Verbose -Message verbose')

        ps.stream_verbose.data_adding += callbacks
        ps.stream_verbose.data_added += callbacks
        ps.stream_verbose.on_completed += callbacks
        ps.state_changed += callbacks

        await ps.invoke()

        assert len(callbacks.data) == 3
        assert isinstance(callbacks.data[0], psrpcore.types.VerboseRecord)
        assert callbacks.data[0].Message == "verbose"
        assert isinstance(callbacks.data[1], psrpcore.types.VerboseRecord)
        assert callbacks.data[1].Message == "verbose"
        assert isinstance(callbacks.data[2], psrpcore.PipelineStateEvent)
        assert len(ps.stream_verbose) == 1

        await ps.stream_verbose.complete()
        assert len(callbacks.data) == 4
        assert isinstance(callbacks.data[3], bool)
        assert callbacks.data[3] is True

        with pytest.raises(ValueError, match="Objects cannot be added to a closed buffer"):
            ps.stream_verbose.append(ps.stream_verbose[0])

        with pytest.raises(ValueError, match="Objects cannot be added to a closed buffer"):
            ps.stream_verbose.insert(0, ps.stream_verbose[0])

        await ps.invoke()
        assert len(callbacks.data) == 5
        assert isinstance(callbacks.data[4], psrpcore.PipelineStateEvent)
        assert len(ps.stream_verbose) == 1


async def test_powershell_blocking_iterator(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    """
    $ps = [PowerShell]::Create()
    $in = [System.Management.Automation.PSDataCollection[PSObject]]::new()
    $out = [System.Management.Automation.PSDataCollection[PSObject]]::new()
    $out.BlockingEnumerator = $true

    Register-ObjectEvent -InputObject $out -EventName Completed -Action { Write-Host "completed" }
    Register-ObjectEvent -InputObject $out -EventName DataAdding -Action { Write-Host "data adding: $args" }
    Register-ObjectEvent -InputObject $out -EventName DataAdded -Action { Write-Host "data added: $args" }

    $ps2 = [PowerShell]::Create()
    $ps2.AddScript('Start-Sleep -Seconds 10; $args[0].Complete()').AddArgument($out)
    $t2 = $ps2.BeginInvoke()

    $ps.AddScript('"1"; Start-Sleep -Seconds 2; "2"; Start-Sleep -Seconds 2; "3"; Start-Sleep -Seconds 2; "4"')
    $t1 = $ps.BeginInvoke($in, $out)

    $out

    $ps2.EndInvoke($t2)
    $ps.EndInvoke($t2)
    """
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)

        out = psrp.AsyncPSDataCollection[t.Any](blocking_iterator=True)

        async def state_callback(event: psrpcore.PipelineStateEvent) -> None:
            await out.complete()

        ps.state_changed += state_callback

        ps.add_script("1, 2, 3, 4, 5")
        task = await ps.invoke_async(output_stream=out)

        result = []
        async for data in out:
            result.append(data)

        assert ps.state == psrpcore.types.PSInvocationState.Completed
        assert result == [1, 2, 3, 4, 5]

        task_out = await task
        assert task_out == []


async def test_powershell_host_call(
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
            $Host.UI.ReadLine()
            $Host.UI.WriteLine("host output")
            """
        )
        actual = await ps.invoke()
        assert actual == ["pipeline line"]
        rp_write_line.assert_not_called()
        ps_write_line.assert_called_once_with("host output")


async def test_powershell_host_call_failure(
    psrp_async_proc: psrp.AsyncProcessInfo, monkeypatch: pytest.MonkeyPatch
) -> None:
    ps_host = psrp.PSHost(ui=psrp.PSHostUI())

    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp, host=ps_host)
        ps.add_script(
            """
            $Host.UI.WriteLine("host output")
            """
        )
        actual = await ps.invoke()
        assert actual == []
        assert len(rp.stream_error) == 0
        assert len(ps.stream_error) == 1
        assert isinstance(ps.stream_error[0], psrpcore.types.ErrorRecord)
        assert str(ps.stream_error[0]) == "NotImplementedError when running HostMethodIdentifier.WriteLine2"


async def test_powershell_complex_commands(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    async with psrp.AsyncRunspacePool(psrp_async_proc) as rp:
        ps = psrp.AsyncPowerShell(rp)
        ps.add_command("Set-Variable").add_parameters(Name="string", Value="foo")
        ps.add_statement()

        ps.add_command("Get-Variable").add_parameter("Name", "string")
        ps.add_command("Select-Object").add_parameter("Property", ["Name", "Value"])
        ps.add_statement()

        ps.add_command("Get-Variable").add_argument("string").add_parameter("ValueOnly", True)
        ps.add_command("Select-Object")
        ps.add_statement()

        actual = await ps.invoke()
        assert len(actual) == 2
        assert isinstance(actual[0], psrpcore.types.PSObject)
        assert actual[0].Name == "string"
        assert actual[0].Value == "foo"
        assert actual[1] == "foo"


async def test_powershell_input_as_iterable(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


async def test_powershell_input_as_async_iterable(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


async def test_powershell_unbuffered_input(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


async def test_powershell_custom_output(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


async def test_powershell_invoke_async(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


async def test_powershell_stop(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


async def test_powershell_stop_async(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""


@pytest.mark.skip
async def test_powershell_connect(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    raise NotImplementedError()


@pytest.mark.skip
async def test_powershell_connect_async(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    raise NotImplementedError()


async def test_run_get_command_meta(psrp_async_proc: psrp.AsyncProcessInfo) -> None:
    a = ""
