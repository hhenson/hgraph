from abc import ABC, abstractmethod
from typing import TypeVar, Generic

ITEM = TypeVar("ITEM")

__all__ = ("Builder",)


class Builder(ABC, Generic[ITEM]):
    """
    The builder is responsible for constructing and initialising the item type it is responsible for.
    It is also responsible for destroying and cleaning up the resources associated to the item.
    These can be thought of as life-cycle methods.
    """

    @abstractmethod
    def make_instance(self, **kwargs) -> ITEM:
        """
        Create a new instance of the item.
        And additional attributes required for construction are passed in as kwargs.
        Actual instance of the builder will fix these args for all instances of builder for the type.
        """

    @abstractmethod
    def release_instance(self, item: ITEM):
        """
        Release the item and it's resources.
        """
