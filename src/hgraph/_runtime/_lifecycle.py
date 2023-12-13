import typing
from abc import abstractmethod
from contextlib import contextmanager
from functools import wraps

__all__ = ("ComponentLifeCycle", "start_guard", "stop_guard", "start_stop_context", "initialise_dispose_context")


class ComponentLifeCycle:
    """
    The Life-cycle and associated method calls are as follows:

    * The component is constructed using the __init__, additional properties may be set after this
      in order to deal with reverse dependencies, etc.
    * The component will have initialise called, in a graph context this call is in topological sort order.
    * The start method is called prior to normal operation of the code, this can perform actions such as schedule node
      evaluation, and should also delegate the call to any components managed by this component.
    * The stop method is called once normal operation of the code is expected to cease. This can be used to stop threads
      or perform any state clean-up required.
    * The dispose method is called once the component is no longer required and in a graph context will be called in
      reverse topological sort order.

    NOTE: The start and stop life-cycle methods can be called numerous times during the life-time of the component.
          The code should ensure that it is able to start again cleanly after stop has be called. Stop is not dispose,
          full clean-up is called only on dispose.
    """

    def __init__(self):
        self._started : bool= False
        self._transitioning: bool = False

    def initialise(self):
        """
        Called once the component has been constructed and prepared.
        Use this life-cycle call to prepare cached data, etc. This is called once after construction and never again.
        If this component creates life-cycle managed components then it should delegate this call to them at this point
        in time, however if the component is provided with life-cycle managed components, then it is NOT the
        responsibility of this component to send intialise or dispose calls.
        """

    @property
    def is_started(self) -> bool:
        """Has this component been started already"""
        return self._started

    @property
    def is_starting(self) -> bool:
        """Is this component in the process of starting"""
        return self._transitioning and not self._started

    @property
    def is_stopping(self) -> bool:
        """Is this component in the process of stopping"""
        return self._transitioning and self._started

    def start(self):
        """
        Perform any actions required to initialise the component such as establishing threads, or scheduling
        initial tasks, etc. This method must ensure the is_started property becomes True once this has been called.
        It is the responsibility of the component to delegate the start life-cycle call to ALL contained life-cycle
        managed components.
        """

    def stop(self):
        """
        Perform any actions required to halt the activities of the component, this may entail activities such as stopping
        threads, resetting state, etc. This method must ensure the is_started property becomes False once this method
        has been called.
        It is the responsibility of the component to delegate the stop life-cycle call to ALL contained life-cycle
        managed components.
        """

    def dispose(self):
        """
        Use this life-cycle call to clean up any resources held. This is called once only at the end of the components
        life-cycle and it is expected the component will be released after this call completes. This ensures
        That any resources that are referenced are cleaned up in a timely fashion.
        When called in the graph context, the order of initialise and dispose are also ensured.
        It is the responsibility of this component to delegate this call to any life-cycle managed components
        that were constructed by this component ONLY, components set on the component are NOT to be delegated to.
        """


def start_guard(fn):
    """
    Use this to decorate the start method, ensures that the component is not started before calling and then
    sets the is_stated property once started.
    """
    @wraps(fn)
    def _start(self: ComponentLifeCycle):
        if self._started:
            return

        try:
            self._transitioning = True
            fn(self)
            self._started = True
        finally:
            self._transitioning = False

    return _start


def stop_guard(fn):
    """
    Use this to decorate to stop method, ensures the component is already started before calling then sets the
    is_started property to False.
    """

    @wraps(fn)
    def _stop(self: ComponentLifeCycle):
        if not self._started:
            return

        try:
            self._transitioning = True
            fn(self)
            self._started = False
        finally:
            self._transitioning = False

    return _stop


LIFE_CYCLABLE = typing.TypeVar("LIFE_CYCLABLE", bound=ComponentLifeCycle)


@contextmanager
def start_stop_context(component: LIFE_CYCLABLE) -> LIFE_CYCLABLE:
    """
    Use the context manager to ensure that the component is started and stopped correctly.
    """
    try:
        component.start()
        yield component
    finally:
        component.stop()


@contextmanager
def initialise_dispose_context(component: LIFE_CYCLABLE) -> LIFE_CYCLABLE:
    """
    Use the context manager to ensure that the component is initialised and disposed correctly.
    """
    try:
        component.initialise()
        yield component
    finally:
        component.dispose()