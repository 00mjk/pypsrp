from cmath import exp

import pytest

from psrp._io import wsman


@pytest.mark.parametrize(
    "args, kwargs, expected",
    [
        (("http://server/",), {}, "http://server/"),
        (("http://server:1234/",), {}, "http://server:1234/"),
        (("http://server/wsman",), {}, "http://server/wsman"),
        (("http://server:5985/wsman",), {}, "http://server:5985/wsman"),
        (("https://server:5986/wsman",), {}, "https://server:5986/wsman"),
        ((("server",), {}, "http://server:5985/wsman")),
        (((), {"server": "server"}, "http://server:5985/wsman")),
        ((("server", "http"), {}, "http://server:5985/wsman")),
        (((), {"server": "server", "scheme": "http"}, "http://server:5985/wsman")),
        ((("server", "http", 5985), {}, "http://server:5985/wsman")),
        ((("server", "http", 80), {}, "http://server:80/wsman")),
        ((("server", "http", 5986), {}, "http://server:5986/wsman")),
        (((), {"server": "server", "scheme": "http", "port": 5985}, "http://server:5985/wsman")),
        ((("server", "http", 5985, "wsman"), {}, "http://server:5985/wsman")),
        ((("server", "https", 5986, "wsman"), {}, "https://server:5986/wsman")),
        ((("server", "http", 5986, "wsman"), {}, "http://server:5986/wsman")),
        (((), {"server": "server", "scheme": "http", "port": 5985, "path": "wsman"}, "http://server:5985/wsman")),
        ((("server",), {"port": 80}, "http://server:80/wsman")),
        ((("server",), {"port": 5985}, "http://server:5985/wsman")),
        ((("server",), {"port": 5986}, "https://server:5986/wsman")),
        ((("2001:0db8:0a0b:12f0:0000:0000:0000:0001",), {}, "http://[2001:db8:a0b:12f0::1]:5985/wsman")),
        ((("2001:db8:a0b:12f0::1",), {}, "http://[2001:db8:a0b:12f0::1]:5985/wsman")),
        ((("FE80::0202:B3FF:FE1E:8329",), {"port": 5986}, "https://[fe80::202:b3ff:fe1e:8329]:5986/wsman")),
    ],
)
def test_connection_info_uri(args, kwargs, expected):
    actual = wsman.WSManConnectionData(*args, **kwargs)
    assert actual.connection_uri == expected
