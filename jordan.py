import asyncio
import os.path
import typing

from psrpcore import Command
from psrpcore.types import ConsoleColor, Coordinates, PipelineResultTypes, Size

from psrp import (
    AsyncPowerShell,
    AsyncProcessConnection,
    AsyncRunspacePool,
    AsyncSSHInfo,
    AsyncWSManConnection,
    PowerShell,
    ProcessConnection,
    RunspacePool,
    WSManConnection,
)
from psrp._host import PSHost, PSHostRawUI, PSHostUI

endpoint = "server2019.domain.test"

script = """
'1'
#sleep 10
'2'
"""


async def async_psrp(connection_info):
    class ReadHost(PSHostUI):
        async def read_line(self):
            return input("press to enter to continue pipeline")

    class RawUI(PSHostRawUI):
        def get_foreground_color(self) -> ConsoleColor:
            return ConsoleColor.White

        def get_background_color(self) -> ConsoleColor:
            return ConsoleColor.Blue

        def get_cursor_position(self) -> Coordinates:
            return Coordinates(10, 20)

        def get_window_position(self) -> Coordinates:
            return Coordinates(0, 0)

        def get_cursor_size(self):
            return 25

        def get_buffer_size(self) -> Size:
            return Size(10, 20)

        def get_window_size(self) -> Size:
            return Size(10, 20)

        def get_window_title(self):
            return "Window Title"

        def get_max_window_size(self) -> Size:
            return Size(10, 20)

        def get_max_physical_window_size(self) -> Size:
            return Size(10, 20)

    host = PSHost(ui=ReadHost(raw_ui=RawUI()))

    async with AsyncRunspacePool(connection_info, host=host) as rp:
        # await rp.reset_runspace_state()
        # await rp.set_max_runspaces(10)
        # await rp.get_available_runspaces()

        # await asyncio.sleep(10)

        async def run_command(time_sec):
            ps = AsyncPowerShell(rp)
            ps.add_script(f'echo "hi"; sleep {time_sec}; echo "end"')
            print(await ps.invoke())

        # done, pending = await asyncio.wait([run_command(1), run_command(2), run_command(3)])
        # for d in done:
        #    print(d.result())

        ps = AsyncPowerShell(rp)
        # ps.add_script('whoami.exe')
        # ps.add_script('echo "hi"; [void]$host.UI.ReadLine(); echo "end"')
        ps.add_script('echo "hi"; write-progress activity status; echo "end"')
        print(await ps.invoke())

    print("exit")
    return


def sync_psrp(connection_info):
    with RunspacePool(connection_info, host=None) as rp:
        ps = PowerShell(rp)
        ps.add_script('echo "hi"; echo "end"')
        print(ps.invoke())


async def async_reconnection(connection_info):
    async with AsyncRunspacePool(connection_info) as rp1:
        print(rp1.pool.runspace_id)
        # await rp1.disconnect()
        # await rp1.connect()

        ps = AsyncPowerShell(rp1)
        ps.add_script(script)
        task = await ps.invoke_async()
        # async for out in ps.invoke():
        #    print(out)

        await rp1.disconnect()

    a = ""

    # async with AsyncRunspacePool(AsyncWSManInfo(f'http://{endpoint}:5985/wsman')) as rp2:
    #    print(rp2.protocol.runspace_id)
    #    await rp2.disconnect()

    a = ""

    async for rp in AsyncRunspacePool.get_runspace_pools(connection_info):
        async with rp:
            a = ""
            for pipeline in rp.create_disconnected_power_shells():
                print(await pipeline.connect())

                a = ""
            a = ""


async def async_main():
    wsman_build_dir = "/home/jborean/dev/wsman-environment/build"
    cert_ca_path = os.path.join(wsman_build_dir, "ca.pem")
    cert_auth_pem = os.path.join(wsman_build_dir, "client_auth.pem")
    cert_auth_key_pem = os.path.join(wsman_build_dir, "client_auth.key")
    cert_auth_key_pass_pem = os.path.join(wsman_build_dir, "client_auth_password.key")

    await asyncio.gather(
        # # Process Scenrios
        # async_psrp(AsyncProcessInfo()),
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://server2019.domain.test:5985/wsman",
        #         username="vagrant-domain@DOMAIN.TEST",
        #         password="VagrantPass1",
        #         auth="negotiate",
        #         # encryption="never",
        #     ),
        # )
        # # SSH Scenarios
        # async_psrp(AsyncSSHInfo("test.wsman.env", username="vagrant", password="vagrant")),
        # # I was hoping this would work but it doesn't, need to play around with this some more locally
        # async_psrp(
        #     AsyncSSHInfo(
        #         "test.wsman.env",
        #         username="vagrant",
        #         password="vagrant",
        #         executable="powershell.exe",
        #         arguments=["-Version", "5.1", "-NoLogo", "-NoProfile", "-s"],
        #     )
        # ),
        # # This does work and it's essentially the same as the subsystem stuff
        # async_psrp(
        #    AsyncSSHInfo(
        #        "test.wsman.env",
        #        username="vagrant",
        #        password="vagrant",
        #        executable="powershell.exe",
        #        arguments=["-Version", "5.1", "-NoLogo", "-s"],
        #    )
        # ),
        # # WSMan Scenarios
        # ## No Proxy ###
        # # http_nego_none_none
        # async_psrp(AsyncWSManInfo(f"http://test.wsman.env:29936/wsman")),
        # # https_nego_none_none
        # async_psrp(AsyncWSManInfo(f"https://test.wsman.env:29900/wsman", verify=cert_ca_path)),
        # # http_ntlm_none_none
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29936/wsman",
        #         auth="ntlm",
        #         username="vagrant-domain@WSMAN.ENV",
        #         password="VagrantPass1",
        #     )
        # ),
        # # https_ntlm_none_none
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29900/wsman",
        #         auth="ntlm",
        #         username="vagrant-domain@WSMAN.ENV",
        #         password="VagrantPass1",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # Anonymous Proxy ###
        # # https_basic_https_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29900/wsman",
        #         proxy="https://squid.wsman.env:3130/",
        #         verify=cert_ca_path,
        #         auth="basic",
        #         username="ansible",
        #         password="Password123!",
        #         encryption="never",
        #     )
        # ),
        # # http_nego_http_anon
        async_psrp(AsyncWSManConnection(f"http://test.wsman.env:29938/wsman", proxy="http://squid.wsman.env:3129/")),
        # # http_nego_https_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29938/wsman", proxy="https://squid.wsman.env:3130/", verify=cert_ca_path
        #     )
        # ),
        # # https_nego_http_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman", proxy="http://squid.wsman.env:3129/", verify=cert_ca_path
        #     )
        # ),
        # # https_nego_https_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman", proxy="https://squid.wsman.env:3130/", verify=cert_ca_path
        #     )
        # ),
        # ## Basic Proxy ###
        # # http_nego_http_basic
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29938/wsman",
        #         proxy="http://squid.wsman.env:3129/",
        #         proxy_auth="basic",
        #         proxy_username="proxy_username",
        #         proxy_password="proxy_password",
        #     )
        # ),
        # # http_nego_https_basic
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29938/wsman",
        #         proxy="https://squid.wsman.env:3130/",
        #         proxy_auth="basic",
        #         proxy_username="proxy_username",
        #         proxy_password="proxy_password",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # https_nego_http_basic
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman",
        #         proxy="http://squid.wsman.env:3129/",
        #         proxy_auth="basic",
        #         proxy_username="proxy_username",
        #         proxy_password="proxy_password",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # https_nego_https_basic
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman",
        #         proxy="https://squid.wsman.env:3130/",
        #         proxy_auth="basic",
        #         proxy_username="proxy_username",
        #         proxy_password="proxy_password",
        #         verify=cert_ca_path,
        #     )
        # ),
        # ## Negotiate Proxy ###
        # # http_nego_http_kerb
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29938/wsman", proxy="http://squid.wsman.env:3135/", proxy_auth="negotiate"
        #     )
        # ),
        # # http_nego_https_kerb
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29938/wsman",
        #         proxy="https://squid.wsman.env:3136/",
        #         proxy_auth="negotiate",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # https_nego_http_kerb
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman",
        #         proxy="http://squid.wsman.env:3135/",
        #         proxy_auth="negotiate",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # https_nego_https_kerb
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman",
        #         proxy="https://squid.wsman.env:3136/",
        #         proxy_auth="negotiate",
        #         verify=cert_ca_path,
        #     )
        # ),
        # ## SOCKS Proxy ###
        # # http_nego_socks5_anon
        # async_psrp(AsyncWSManInfo(f"http://test.wsman.env:29938/wsman", proxy="socks5://127.0.0.1:53547/")),
        # # https_nego_socks5_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29902/wsman", proxy="socks5://127.0.0.1:53547/", verify=cert_ca_path
        #     )
        # ),
        # # http_nego_socks5h_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://remote-res.wsman.env:29938/wsman",
        #         proxy="socks5h://127.0.0.1:53547/",
        #         auth="ntlm",
        #         username="vagrant-domain@WSMAN.ENV",
        #         password="VagrantPass1",
        #     )
        # ),
        # # https_nego_socks5h_anon
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://remote-res.wsman.env:29902/wsman",
        #         proxy="socks5h://127.0.0.1:53547/",
        #         auth="ntlm",
        #         username="vagrant-domain@WSMAN.ENV",
        #         password="VagrantPass1",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # http_basic_none_none
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"http://test.wsman.env:29936/wsman",
        #         auth="basic",
        #         username="ansible",
        #         password="Password123!",
        #         encryption="never",
        #     )
        # ),
        # # https_basic_none_none
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29900/wsman",
        #         auth="basic",
        #         username="ansible",
        #         password="Password123!",
        #         verify=cert_ca_path,
        #     )
        # ),
        # # https_cert_none_none
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29900/wsman",
        #         auth="certificate",
        #         verify=cert_ca_path,
        #         certificate_pem=cert_auth_pem,
        #         certificate_key_pem=cert_auth_key_pem,
        #     )
        # ),
        # # https_certpass_none_none
        # async_psrp(
        #     AsyncWSManInfo(
        #         f"https://test.wsman.env:29900/wsman",
        #         auth="certificate",
        #         verify=cert_ca_path,
        #         certificate_pem=cert_auth_pem,
        #         certificate_key_pem=cert_auth_key_pass_pem,
        #         certificate_password="password",
        #     )
        # ),
        # # async_reconnection(AsyncWSManInfo(f"http://{endpoint}:5985/wsman")),
    )


import io
import logging
import os
import xml.dom.minidom

from psrpcore._payload import unpack_fragment, unpack_message
from ruamel import yaml


class MyFileLogger(logging.FileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buffer = {}

    def emit(self, record):
        if record.msg.startswith("WSMan"):
            pretty_xml = self._pretty_xml(record.args[0])
            msg = "### %s\n```xml\n%s\n```\n" % (record.msg, pretty_xml)

        elif record.msg.startswith("PSRP"):
            pretty_psrp = self._pretty_psrp(record.args[0])
            msg = "### %s\n```yaml\n%s\n```\n" % (record.msg, pretty_psrp)

        else:
            msg = record.msg % record.args

        record.args = ()
        record.msg = msg
        super().emit(record)

    def _pretty_xml(self, xml_data):
        dom = xml.dom.minidom.parseString(xml_data)
        pretty_xml = dom.documentElement.toprettyxml()
        # remove the weird newline issue:
        pretty_xml = os.linesep.join([s for s in pretty_xml.splitlines() if s.strip()])
        return pretty_xml

    def _pretty_psrp(self, data):
        msgs = []

        while data:
            fragment = unpack_fragment(data)
            data = data[21 + len(fragment.data) :]
            buffer = self._buffer.setdefault(fragment.object_id, [])
            buffer.append(fragment)

            msg = None
            if fragment.end:
                del self._buffer[fragment.object_id]
                msg_data = b"".join([f.data for f in buffer])
                raw_msg = unpack_message(msg_data)
                message_type = raw_msg.message_type.value
                msg = {
                    "MessageType": f"{raw_msg.message_type.name} - {message_type} 0x{message_type:08X}",
                    "RPID": str(raw_msg.rpid) if raw_msg.rpid else None,
                    "PID": str(raw_msg.pid) if raw_msg.pid else None,
                    "Data": yaml.scalarstring.PreservedScalarString(self._pretty_xml(raw_msg.data.decode())),
                }

            msgs.append(
                {
                    "ObjectID": fragment.object_id,
                    "FragmentId": fragment.fragment_id,
                    "Start": fragment.start,
                    "End": fragment.end,
                    "Msg": msg,
                }
            )

        return self._to_yaml(msgs)

    def _to_yaml(self, data):
        y = yaml.YAML()
        y.default_flow_style = False
        stream = io.StringIO()

        y.dump(data, stream)

        return stream.getvalue()


"""
# Open Runspace Pool

WSMan
    Client -> Server
        Create - SessionCapability, InitRunspacePool

    Server -> Client
        CreateResponse

    Client -> Server
        Send - Any fragments that didn't fit into Create

    Server -> Client
        SendResponse

    Client -> Server (Keeps on running until the runspace is closed)
        Receive

    Server -> Client
        ReceiveResponse - SessionCapability, ApplicationPrivateData, RunspacePoolState

OutOfProc
    Client -> Server
        Data - SessionCapability, InitRunspacePool

    Server -> Client
        Data - SessionCapability, ApplicationPrivateData, RunspacePoolState
        DataAck



# Close Runspace Pool

Must stop all pipelines first

WSMan
    Client -> Server
        Delete

    Server -> Client
        DeleteResponse

OutOfProc
    Client -> Server
        Close

    Server -> Client
        Data - RunspacePoolState - 4 (Closing)
        Data - RunspacePoolState - 3 (Closed)
        CloseAck



# Disconnect Runspace Pool

WSMan
    Client -> Server
        Disconnect - Contains IdleTimeout and BufferMode but no PSRP message

    Server -> Client
        DisconnectResponse

OutOfProc
    N/A



# Reconnect Runspace Pool

WSMan
    Client -> Server
        Connect - SessionCapability, ConnectRunspacePool

    Server -> Client
        ConnectResponse - SessionCapability, RunspacePoolInitData

    Client -> Server  # Keeps on running until the runspace is closed
        Receive

    Server -> Client
        ReceiveResponse - ApplicationPrivateData

OutOfProc
    N/A



# Connect Runspace Pool

WSMan
    Client -> Server
        Connect - SessionCapability, ConnectRunspacePool

    Server -> Client
        ConnectResponse - Some fragments

    Client -> Server
        Receive

    Server -> Client
        ReceiveResponse

OutOfProc
    N/A



# Create a pipeline

WSMan
    Client -> Server
        Command - CreatePipeline

    Server -> Client
        CommandResponse

OutOfProc
    Client -> Server
        Command

    Server -> Client
        CommandAck

    Client -> Server
        Data - CreatePipeline

    Server -> Client
        DataAck



# Pipeline end

WSMan
    Server -> Client
        ReceiveResponse - PipelineState

OutOfProc
    Server -> Client
        Data - PipelineState



# Pipeline cleanup

WSMan
    Client -> Server
        Signal - http://schemas.microsoft.com/wbem/wsman/1/windows/shell/signal/terminate

    Server -> Client
        SignalResponse

OutOfProc
    Client -> Server
        Close

    Server -> Client
        CloseAck



# Stop a pipeline

Still runs the cleanup step. Both have a PipelineState stopped response

WSMan: - Still send signal to cleanup/terminate afterwards
    Client -> Server
        Signal - powershell/signal/crtl_c

    Server -> Client
        SignalResponse
"""


def sync_main():

    log_path = "/home/jborean/dev/pypsrp/wsman.md"
    if os.path.exists(log_path):
        os.remove(log_path)

    handler = MyFileLogger(log_path)
    handler.setFormatter(logging.Formatter("%(message)s"))
    for l in ["psrp.io.wsman", "psrp.connection_info"]:
        log = logging.getLogger(l)
        log.setLevel(logging.DEBUG)
        log.addHandler(handler)

    info = WSManConnection("http://server2019.domain.test:5985/wsman")
    with RunspacePool(info) as pool:
        # ps = PowerShell(pool)
        # ps.add_script("echo 'hi'")
        # ps.invoke()
        pool.disconnect()

    for p in RunspacePool.get_runspace_pool(info):
        with p:
            a = ""


def sync_main2():
    wsman_build_dir = "/home/jborean/dev/wsman-environment/build"
    cert_ca_path = os.path.join(wsman_build_dir, "ca.pem")
    cert_auth_pem = os.path.join(wsman_build_dir, "client_auth.pem")
    cert_auth_key_pem = os.path.join(wsman_build_dir, "client_auth.key")
    cert_auth_key_pass_pem = os.path.join(wsman_build_dir, "client_auth_password.key")

    connections = [
        # # Process Scenrios
        # ProcessInfo(),
        # # WSMan Scenarios
        # ## No Proxy ###
        # # http_nego_none_none
        # WSManInfo(f"http://test.wsman.env:29936/wsman"),
        # # https_nego_none_none
        # WSManInfo(f"https://test.wsman.env:29900/wsman", verify=cert_ca_path),
        # # http_ntlm_none_none
        # WSManInfo(
        #     f"http://test.wsman.env:29936/wsman",
        #     auth="ntlm",
        #     username="vagrant-domain@WSMAN.ENV",
        #     password="VagrantPass1",
        # ),
        # # https_ntlm_none_none
        # WSManInfo(
        #     f"https://test.wsman.env:29900/wsman",
        #     auth="ntlm",
        #     username="vagrant-domain@WSMAN.ENV",
        #     password="VagrantPass1",
        #     verify=cert_ca_path,
        # ),
        # ## Anonymous Proxy ###
        # # http_nego_http_anon
        # WSManInfo(f"http://test.wsman.env:29938/wsman", proxy="http://squid.wsman.env:3129/"),
        # # http_nego_https_anon
        # WSManInfo(f"http://test.wsman.env:29938/wsman", proxy="https://squid.wsman.env:3130/", verify=cert_ca_path),
        # # https_nego_http_anon
        # WSManInfo(f"https://test.wsman.env:29902/wsman", proxy="http://squid.wsman.env:3129/", verify=cert_ca_path),
        # # https_nego_https_anon
        # WSManInfo(f"https://test.wsman.env:29902/wsman", proxy="https://squid.wsman.env:3130/", verify=cert_ca_path),
        # ## Basic Proxy ###
        # # http_nego_http_basic
        # WSManInfo(
        #     f"http://test.wsman.env:29938/wsman",
        #     proxy="http://squid.wsman.env:3129/",
        #     proxy_auth="basic",
        #     proxy_username="proxy_username",
        #     proxy_password="proxy_password",
        # ),
        # # http_nego_https_basic
        # WSManInfo(
        #     f"http://test.wsman.env:29938/wsman",
        #     proxy="https://squid.wsman.env:3130/",
        #     proxy_auth="basic",
        #     proxy_username="proxy_username",
        #     proxy_password="proxy_password",
        #     verify=cert_ca_path,
        # ),
        # # https_nego_http_basic
        # WSManInfo(
        #     f"https://test.wsman.env:29902/wsman",
        #     proxy="http://squid.wsman.env:3129/",
        #     proxy_auth="basic",
        #     proxy_username="proxy_username",
        #     proxy_password="proxy_password",
        #     verify=cert_ca_path,
        # ),
        # # https_nego_https_basic
        # WSManInfo(
        #     f"https://test.wsman.env:29902/wsman",
        #     proxy="https://squid.wsman.env:3130/",
        #     proxy_auth="basic",
        #     proxy_username="proxy_username",
        #     proxy_password="proxy_password",
        #     verify=cert_ca_path,
        # ),
        # ## Negotiate Proxy ###
        # # http_nego_http_kerb
        # WSManInfo(f"http://test.wsman.env:29938/wsman", proxy="http://squid.wsman.env:3135/", proxy_auth="negotiate"),
        # # http_nego_https_kerb
        # WSManInfo(
        #     f"http://test.wsman.env:29938/wsman",
        #     proxy="https://squid.wsman.env:3136/",
        #     proxy_auth="negotiate",
        #     verify=cert_ca_path,
        # ),
        # # https_nego_http_kerb
        # WSManInfo(
        #     f"https://test.wsman.env:29902/wsman",
        #     proxy="http://squid.wsman.env:3135/",
        #     proxy_auth="negotiate",
        #     verify=cert_ca_path,
        # ),
        # # https_nego_https_kerb
        # WSManInfo(
        #     f"https://test.wsman.env:29902/wsman",
        #     proxy="https://squid.wsman.env:3136/",
        #     proxy_auth="negotiate",
        #     verify=cert_ca_path,
        # ),
        # ## SOCKS Proxy ###
        # # http_nego_socks5_anon
        # WSManInfo(f"http://test.wsman.env:29938/wsman", proxy="socks5://127.0.0.1:53547/"),
        # # https_nego_socks5_anon
        # WSManInfo(f"https://test.wsman.env:29902/wsman", proxy="socks5://127.0.0.1:53547/", verify=cert_ca_path),
        # # http_nego_socks5h_anon
        # WSManInfo(
        #     f"http://remote-res.wsman.env:29938/wsman",
        #     proxy="socks5h://127.0.0.1:53547/",
        #     auth="ntlm",
        #     username="vagrant-domain@WSMAN.ENV",
        #     password="VagrantPass1",
        # ),
        # # https_nego_socks5h_anon
        # WSManInfo(
        #     f"https://remote-res.wsman.env:29902/wsman",
        #     proxy="socks5h://127.0.0.1:53547/",
        #     auth="ntlm",
        #     username="vagrant-domain@WSMAN.ENV",
        #     password="VagrantPass1",
        #     verify=cert_ca_path,
        # ),
        # # http_basic_none_none
        # WSManInfo(
        #     f"http://test.wsman.env:29936/wsman",
        #     auth="basic",
        #     username="ansible",
        #     password="Password123!",
        #     encryption="never",
        # ),
        # # https_basic_none_none
        # WSManInfo(
        #     f"https://test.wsman.env:29900/wsman",
        #     auth="basic",
        #     username="ansible",
        #     password="Password123!",
        #     verify=cert_ca_path,
        # ),
        # # https_cert_none_none
        # WSManInfo(
        #     f"https://test.wsman.env:29900/wsman",
        #     auth="certificate",
        #     verify=cert_ca_path,
        #     certificate_pem=cert_auth_pem,
        #     certificate_key_pem=cert_auth_key_pem,
        # ),
        # # https_certpass_none_none
        # WSManInfo(
        #     f"https://test.wsman.env:29900/wsman",
        #     auth="certificate",
        #     verify=cert_ca_path,
        #     certificate_pem=cert_auth_pem,
        #     certificate_key_pem=cert_auth_key_pass_pem,
        #     certificate_password="password",
        # ),
    ]

    for conn in connections:
        sync_psrp(conn)


asyncio.run(async_main())
# sync_main()


"""


def normal_test():
    with WSMan(endpoint) as wsman, WinRS(wsman) as shell:
        proc = Process(shell, 'cmd.exe', ['/c', 'echo hi'])
        proc.invoke()
        proc.signal(SignalCode.TERMINATE)
        print("STDOUT:\n%s\nSTDERR:\n%s\nRC: %s" % (proc.stdout.decode(), proc.stderr.decode(), proc.rc))

    with WSMan(endpoint) as wsman, RunspacePool(wsman) as rp:
        ps = PowerShell(rp)
        ps.add_script('echo "hi"')
        output = ps.invoke()
        print("\nPSRP: %s" % output)


async def async_test():
    #async with AsyncWSMan(endpoint) as wsman, AsyncWinRS(wsman) as shell:
    #    proc = AsyncProcess(shell, 'cmd.exe', ['/c', 'echo hi'])
    #    await proc.invoke()
    #    await proc.signal(SignalCode.TERMINATE)
    #    print("STDOUT:\n%s\nSTDERR:\n%s\nRC: %s" % (proc.stdout.decode(), proc.stderr.decode(), proc.rc))

    async with AsyncWSMan(endpoint) as wsman, AsyncRunspacePool(wsman) as rp:
        ps = AsyncPowerShell(rp)
        ps.add_script('echo "hi"')
        output = await ps.invoke()
        print("\nPSRP: %s" % output)


async def async_process():
    async with PowerShellProcess() as proc, AsyncRunspacePool(proc) as rp:
        ps = AsyncPowerShell(rp)
        ps.add_script('echo "hi"')
        output = await ps.invoke()
        print("\nPSRP: %s" % output)


async def async_h2():
    from psrp.winrs import (
        AsyncWinRS,
    )

    connection_uri = 'http://server2019.domain.test:5985/wsman'

    async with AsyncWinRS(connection_uri) as shell:
        proc = await shell.execute('powershell.exe', ['-Command', 'echo "hi"'])
        async with proc:
            data = await proc.stdout.read()
            await proc.wait()
            print(data)


async def async_psrp():
    from psrp.powershell import (
        AsyncRunspacePool,
    )
    rp = AsyncRunspacePool()
    rp.open()


#normal_test()
#print()

#asyncio.run(async_test())
#print()
#asyncio.run(async_process())

asyncio.run(async_psrp())
"""
