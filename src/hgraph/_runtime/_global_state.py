from typing import Optional, Any


__all__ = ("GlobalState",)


class GlobalState(object):
    """
    Provide a global state that can be accessed bia all components of the graph and can be made to be present at the
    end of graph evaluation. This can be useful to provide debugging information as well as directory services.
    """

    _instance: Optional["GlobalState"] = None

    @staticmethod
    def instance() -> "GlobalState":
        if GlobalState._instance is None:
            GlobalState._instance = GlobalState()
        return GlobalState._instance

    @staticmethod
    def reset():
        GlobalState._instance = None

    def __init__(self, **kwargs):
        self._state: dict[str, Any] = dict(**kwargs)

    def __getitem__(self, item: str) -> Any:
        return self._state[item]

    def __setitem__(self, key: str, value: Any):
        self._state[key] = value

    def setdefault(self, key: str, default: Any) -> Any:
        return self._state.setdefault(key, default)

    def __delitem__(self, key: str):
        del self._state[key]

    def __contains__(self, item: str):
        return item in self._state

    def __iter__(self):
        return iter(self._state)

    def __len__(self):
        return len(self._state)

    def __repr__(self):
        return f"GlobalState({self._state})"

    def __str__(self):
        return str(self._state)

    def __eq__(self, other):
        return self._state == other._state

    def __ne__(self, other):
        return self._state != other._state

    def __bool__(self):
        return bool(self._state)

    def __getattr__(self, item: str) -> Any:
        return self._state[item]

    def __setattr__(self, key: str, value: Any):
        if key == "_state":
            super().__setattr__(key, value)
        else:
            self._state[key] = value

    def __delattr__(self, item: str):
        del self._state[item]

    def __dir__(self):
        return dir(self._state)

    def __getstate__(self):
        return self._state

    def __setstate__(self, state):
        self._state = state

    def get(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)
