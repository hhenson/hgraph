from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, Tuple


@dataclass
class UnitSystem:
    __instance__ = None

    __dimensions__: ClassVar[dict[str, "Dimension"]] = dict()
    __derived_dimensions__: ClassVar[dict[tuple[tuple["Dimension", int], ...], "Dimension"]] = dict()

    __primary_units__: ClassVar[dict[str, "Unit"]] = dict()
    __derived_units__: ClassVar[dict[Tuple["Unit", Decimal], "Unit"]] = dict()
    __complex_units__: ClassVar[dict[tuple[tuple["Unit", int], ...], "Unit"]] = dict()

    @classmethod
    def instance(cls):
        return cls.__instance__

    def __enter__(self):
        if self.__class__.__instance__ is not None:
            raise ValueError(f"UnitSystems are not stackable contexts")

        self.__class__.__instance__ = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.__class__.__instance__ == self

        self.__class__.__instance__ = None

    def __setattr__(self, key, value):
        if value.name != key:
            object.__setattr__(value, 'name', key)

        object.__setattr__(self, key, value)
