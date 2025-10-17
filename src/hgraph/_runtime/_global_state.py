from typing import Optional, Any

__all__ = ("GlobalState",)


class GlobalState(object):
    _root_instance: Optional["GlobalState"] = None
    """
    Provide a global state that can be accessed via all components of the graph and can be made to be present at the
    end of graph evaluation. This can be useful to provide debugging information as well as directory services.
    """

    _instance: Optional["GlobalState"] = None

    @staticmethod
    def init_multithreading():
        from threading import local

        if isinstance(GlobalState._instance, local):
            return

        current_instance = GlobalState._instance

        GlobalState._root_instance = current_instance if isinstance(current_instance, GlobalState) else None

        GlobalState._instance = local()  # type: ignore
        # Ensure methods remain static when swapped for MT variants
        GlobalState.instance = staticmethod(GlobalState.instance_mt)  # type: ignore
        GlobalState.set_instance = staticmethod(GlobalState.set_instance_mt)  # type: ignore
        GlobalState.has_instance = staticmethod(GlobalState.has_instance_mt)  # type: ignore

        if current_instance is not None:
            GlobalState.set_instance(current_instance)

    @staticmethod
    def instance() -> "GlobalState":
        if GlobalState._instance is None:
            raise RuntimeError("No global state is present")  # default constructing one is very bad for tests
        return GlobalState._instance

    @staticmethod
    def set_instance(self):
        GlobalState._instance = self

    @staticmethod
    def has_instance() -> bool:
        return GlobalState._instance is not None

    @staticmethod
    def instance_mt() -> "GlobalState":
        gs = GlobalState._instance.__dict__.get("self")
        if gs is None:
            if GlobalState._root_instance is not None:
                return GlobalState._root_instance
            raise RuntimeError("No global state is present")  # default constructing one is very bad for tests
        return gs

    @staticmethod
    def set_instance_mt(self):
        GlobalState._instance.self = self
        GlobalState._root_instance = self

    @staticmethod
    def has_instance_mt() -> bool:
        return GlobalState._instance.__dict__.get("self") is not None

    @staticmethod
    def reset():
        GlobalState.set_instance(None)

    def __enter__(self):
        self._previous = GlobalState.instance() if GlobalState.has_instance() else None
        GlobalState.set_instance(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        GlobalState.set_instance(self._previous)
        self._previous = None

    def __init__(self, **kwargs):
        self._state: dict[str, Any] = dict(**kwargs)
        self._previous: GlobalState | None = None

    def _get_combined_dict(self) -> dict[str, Any]:
        if self._previous is None:
            return self._state
        else:
            return self._state | self._previous._get_combined_dict()

    def __getitem__(self, item: str) -> Any:
        if item not in self._state and self._previous is not None:
            return self._previous[item]
        return self._state[item]

    def __setitem__(self, key: str, value: Any):
        self._state[key] = value

    def setdefault(self, key: str, default: Any) -> Any:
        return self._state.setdefault(key, default)

    def __delitem__(self, key: str):
        if key not in self._state and self._previous is not None:
            del self._previous[key]
        del self._state[key]

    def __contains__(self, item: str):
        if item in self._state:
            return True
        else:
            return False if self._previous is None else self._previous.__contains__(item)

    def __iter__(self):
        return iter(self._get_combined_dict())

    def __len__(self):
        if self._previous is None:
            return len(self._state)
        else:
            return len(self._state) + len(self._previous)

    def __repr__(self):
        return (
            f"GlobalState({self._state}{(', previous=' + repr(self._previous)) if self._previous is not None else ''})"
        )

    def __str__(self):
        return str(self._get_combined_dict())

    def __bool__(self):
        return bool(self._state) or bool(self._previous)

    def __getattr__(self, item: str) -> Any:
        if item not in self._state and self._previous is not None:
            return self._previous.__getattr__(item)
        return self._state[item]

    def __setattr__(self, key: str, value: Any):
        if key == "_state":
            super().__setattr__(key, value)
        else:
            self._state[key] = value

    def __delattr__(self, item: str):
        if item not in self._state and self._previous is not None:
            self._previous.__delattr__(item)
        del self._state[item]

    def __dir__(self):
        return dir(self._get_combined_dict())

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._state:
            return self._state[key]
        if self._previous is None:
            return default
        else:
            return self._previous.get(key, default)

    def setdefault(self, key: str, default: Any) -> Any:
        if key in self._state:
            return self._state[key]
        if self._previous is not None:
            return self._previous.setdefault(key, default)
        else:
            self._state[key] = default
            return default

    def keys(self):
        return self._state.keys()

    def values(self):
        return self._state.values()

    def items(self):
        return self._state.items()

    def pop(self, key: str, default: Any = None) -> Any:
        return self._state.pop(key, default)
