
__all__ = ['WiringContext', "StrictWiringContext", "WIRING_CONTEXT"]


class StrictWiringContext:
    """Provide a wrapper of the WiringContext that enforced that the context is active"""

    __slots__ = tuple()

    def __getattr__(self, item):
        if not WiringContext.is_active():
            raise RuntimeError("No wiring context is active")
        return WiringContext.__getattr__(item, )

    def __setattr__(self, key, value):
        if not WiringContext.is_active():
            raise RuntimeError("No wiring context is active")
        WiringContext.__setattr__(key, value, )


class WiringContext:
    """
    Provides the ability to store context variables using a stack.
    A value provided in a lower level of the stack can be overridden and when the context is existed the original
    value will be restored.
    This is used during the wiring phase to track useful state to assist with raising errors with better context.
    """
    __slots__ = ('_state',)
    _stack: [dict] = []

    def __init__(self, **kwargs):
        super().__init__()
        super().__setattr__('_state', dict(**kwargs))

    def __enter__(self):
        WiringContext._stack.append(super().__getattribute__("_state"))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        WiringContext._stack.pop()

    @classmethod
    def is_active(cls):
        return bool(cls._stack)

    def __getattr__(self, item):
        if self._stack:
            for d in reversed(self._stack):
                if item in d:
                    return d[item]
        # Else we return None by default

    def __setattr__(self, key, value):
        if self._stack:
            self._stack[-1][key] = value
        else:
            pass  # By default we just ignore, use strict context to raise errors


WIRING_CONTEXT = WiringContext()
STRICT_WIRING_CONTEXT = StrictWiringContext()