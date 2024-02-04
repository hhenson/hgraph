from abc import abstractmethod

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass


class ServiceInterfaceNodeClass(BaseWiringNodeClass):

    @abstractmethod
    def full_path(self, user_path: str | None) -> str:
        """The full path of the service interface"""
