from abc import ABC, abstractmethod
from typing import Callable, Any


class SerialiserHandler(ABC):

    @abstractmethod
    def matches(self, tp: type) -> float:
        """
        Returns the priority of the match.
        0.0 => no match
        1.0 => perfect match.

        The higher the match, the more precise the rendering solution.
        """

    @abstractmethod
    def serialiser(self, tp: type) -> Callable:
        """
        Return callable that will accept the value to be serialised as well as the builder to apply the value
        to.
        """

    @abstractmethod
    def deserialiser(self, tp: Any) -> Callable:
        """
        Return callable that will accept the serialised value, along with the current index.
        """


class SerialiseModel(ABC):

    @classmethod
    def register_handler(cls, handler):
        if not hasattr(cls, "_handlers"):
            cls._handlers = []
        cls._handlers.append(handler)

    @classmethod
    def resolve_handler(cls, tp: type) -> type | None:
        current_resolution = 0.0
        resolved_handler = None
        for handler in cls._handlers:
            if (resolution := handler.matches(tp)) != 0.0:
                if current_resolution < resolution:
                    current_resolution = resolution
                    resolved_handler = handler
                if current_resolution == 1.0:
                    break
        return resolved_handler

    @abstractmethod
    def make_serialiser(self) -> Callable[[Any], Any]: ...

    @abstractmethod
    def make_deserialiser(self): ...


class JSonSerialiserModel(SerialiseModel): ...


class JSonSerialiserHandler_Scalar(SerialiserHandler):

    def matches(self, tp: type) -> bool:
        pass

    def serialiser(self, tp: type) -> Callable[[Any, Any], None]:
        pass

    def deserialiser(self, tp: Any) -> Callable[[Any, Any], Any]:
        pass
