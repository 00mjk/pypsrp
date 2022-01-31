# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import functools
import inspect
import typing


def iscoroutinefunction(
    value: typing.Any,
) -> bool:
    """Checks if a function is a coroutine even when wrapped by functools."""
    # FUTURE: Remove once Python 3.8 is minimum
    while isinstance(value, functools.partial):
        value = value.func

    return inspect.iscoroutinefunction(value)
