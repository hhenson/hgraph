from hgraph._types._scalar_value import ScalarValue


__all__ = ("PythonScalarValue", "PythonKeyableScalarValue")


class PythonScalarValue(ScalarValue):
    """
    A light-weight wrapper to provide the ScalarValue implementation over Python value types
    """

    def __init__(self, tp: type, value: "Any"):
        self._tp = tp
        self._value = value

    def __str__(self) -> str:
        return str(self._value)

    def __eq__(self, other: "ScalarValue") -> bool:
        if isinstance(other, PythonScalarValue) and self._tp == other._tp:
            return self._value == other._value
        else:
            return False

    def __copy__(self) -> "ScalarValue":
        return PythonScalarValue(self._tp, self._value)

    def __lt__(self, other):
        if isinstance(other, PythonScalarValue):
            return self._value < other._value if self._tp == other._tp else self._tp < other._tp
        else:
            return False

    def cast(self, tp: type):
        if tp == object:
            return self._value
        if tp == self._tp:
            return self._value
        else:
            raise TypeError(f"Cannot cast {self._tp} to {tp}")


class PythonKeyableScalarValue(PythonScalarValue):
    """
    A scalar value that can be used as a key in a dictionary.
    """

    def __hash__(self) -> int:
        return hash(self._value)
