# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

"""
WSMan library for Python.

This is an internal library for WSMan for use with pypsrp. I'm trying to
separate all the logic into it's own package so I could eventually split it
out into it's own library.

The HTTP library used is based on `httpx`_ but uses it's own transport and
relies on currently private implementations in httpcore to support WSMan
specific components. The goal is to remove the reliance on private details but
for now we pin to the minor version.

There are 2 things that need to happen before we can do that:

* Make the HTTPConnection classes public `#272`_.
* Expose the backends so we can manually connect to the socket before sending a request `#273`_.
  * This also fixes the StartTLS issues when already in TLS (HTTPS proxy with HTTPS) `#254`_.

.. _httpx:
    https://github.com/encode/httpx

.. _httpcore:
    https://github.com/encode/httpcore

.. _#254:
    https://github.com/encode/httpcore/issues/254

.. _#272:
    https://github.com/encode/httpcore/issues/272

.. _#273:
    https://github.com/encode/httpcore/issues/273
"""

# from ._auth import AuthHandler, BasicAuth, NegotiateAuth, NoAuth
# from ._transport import WSManTransport

# __all__ = [
#     "AuthHandler",
#     "BasicAuth",
#     "NegotiateAuth",
#     "NoAuth",
#     "WSManTransport",
# ]
