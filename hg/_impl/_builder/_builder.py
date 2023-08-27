from abc import ABC, abstractmethod
from typing import TypeVar, Generic

ITEM = TypeVar('ITEM')


class Builder(ABC, Generic[ITEM]):
    """
    The builder is responsible for constructing and initialising the item type it is responsible for.
    It is also responsible for destroying and cleaning up the resources associated to the item.
    These can be thought of as life-cycle methods.
    """

    @abstractmethod
    def make_instance(self) -> ITEM:
        """
        Create a new instance of the item.
        """

    @abstractmethod
    def release_instance(self, item: ITEM):
        """
        Release the item and it's resources.
        """

