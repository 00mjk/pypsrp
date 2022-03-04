# -*- coding: utf-8 -*-
# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import typing as t


class PSRPError(Exception):
    """Base error class for psrp operations."""


class WSManFaultCode(enum.IntEnum):
    """WSMan error codes.

    A collection of known WSMan error codes as retrieved from `wsman.h`_ in the
    Windows SDK. This is built based on the WSManFault exceptions that have
    been defined.

    .. wsman.h:
        https://github.com/tpn/winsdk-10/blob/master/Include/10.0.16299.0/um/wsmandisp.h
    """

    OPERATION_ABORTED = 0x000003E3
    OPERATION_TIMED_OUT = 0x80338029
    SERVICE_STREAM_DISCONNECTED = 0x803381DE
    UNKNOWN = 0x8033FFFF


class _WSManFaultRegistry(type):
    __registry: t.Dict[int, "_WSManFaultRegistry"] = {}

    def __init__(
        cls,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        code = getattr(cls, "CODE", None)
        if code is not None:
            cls.__registry[int(code)] = cls

    def __call__(
        cls,
        code: t.Optional[int] = None,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> t.Any:
        new_cls = cls
        if code is None:
            code = getattr(cls, "CODE", WSManFaultCode.UNKNOWN)
        else:
            new_cls = cls.__registry.get(code, cls)

        return super(_WSManFaultRegistry, new_cls).__call__(code=code, *args, **kwargs)


class WSManFault(PSRPError, metaclass=_WSManFaultRegistry):
    CODE = WSManFaultCode.UNKNOWN
    MESSAGE = "Unknown WS-Management fault."

    def __init__(
        self,
        code: t.Optional[int] = None,
        machine: t.Optional[str] = None,
        reason: t.Optional[str] = None,
        provider: t.Optional[str] = None,
        provider_path: t.Optional[str] = None,
        provider_fault: t.Optional[str] = None,
    ):
        self.code = code
        self.machine = machine
        self.reason = reason
        self.provider = provider
        self.provider_path = provider_path
        self.provider_fault = provider_fault

    @property
    def message(self) -> str:
        error_details = []
        if self.code:
            error_details.append("Code: %s" % self.code)

        if self.machine:
            error_details.append("Machine: %s" % self.machine)

        if self.reason:
            error_details.append("Reason: %s" % self.reason)

        if self.provider:
            error_details.append("Provider: %s" % self.provider)

        if self.provider_path:
            error_details.append("Provider Path: %s" % self.provider_path)

        if self.provider_fault:
            error_details.append("Provider Fault: %s" % self.provider_fault)

        if len(error_details) == 0:
            error_details.append("No details provided")

        return "Received a WSManFault message. (%s)" % ", ".join(error_details)

    def __str__(self) -> str:
        return self.message


class OperationAborted(WSManFault):
    # Not a WSMan NtStatus code but is returned on an active Receive request when the shell is closed.
    CODE = WSManFaultCode.OPERATION_ABORTED
    MESSAGE = "The I/O operation has been aborted because of either a thread exit or an application request."


class OperationTimedOut(WSManFault):
    CODE = WSManFaultCode.OPERATION_TIMED_OUT
    MESSAGE = "The WS-Management service cannot complete the operation within the time specified in OperationTimeout."


class ServiceStreamDisconnected(WSManFault):
    CODE = WSManFaultCode.SERVICE_STREAM_DISCONNECTED
    MESSAGE = "The WS-Management service cannot process the request because the stream is currently disconnected."


class PipelineFailed(PSRPError):
    """A pipeline failed to start/complete."""


class PipelineStopped(PSRPError):
    """A pipeline was stopped."""
