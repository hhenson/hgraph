"""
Provide helper functions for wiring.
"""

from typing import Callable, TypeVar

from hgraph._types import OUT, HgTypeMetaData
from hgraph._wiring._wiring_errors import CustomMessageWiringError

__all__ = ("get_service_inputs", "set_service_output")


def get_service_inputs(
    path: str | None,
    service: Callable,
    __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
    **kwargs,
):
    """
    Extract the service inputs from a service interface.
    This is used when implementing a service with multiple interfaces, for example:

    @service_impl(interfaces=(submit, receive, subscribe))
    def impl(path: str):
        submissions: TSD[int, TS[int]] = get_service_inputs(path, submit).ts

    """
    try:
        return service.wire_impl_inputs_stub(path, __pre_resolved_types__=__pre_resolved_types__, **kwargs)
    except AttributeError:
        raise CustomMessageWiringError(
            f"The provided service for path: '{path}', does not appear to be a service interface: {service}"
        )


def set_service_output(path: str | None, service: Callable, out: OUT, __pre_resolved_types__=None, **kwargs):
    """
    Extract the service inputs from a service interface.
    This is used when implementing a service with multiple interfaces, for example:

    @service_impl(interfaces=(submit, receive, subscribe))
    def impl(path: str):
        ...
        set_service_output(path, receive, items)
    """
    try:
        service.wire_impl_out_stub(path, out, __pre_resolved_types__=None, **kwargs)
    except AttributeError:
        raise CustomMessageWiringError(
            f"The provided service for path: '{path}', does not appear to be a service interface: {service}"
        )
