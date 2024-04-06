from abc import abstractmethod

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass


class ServiceInterfaceNodeClass(BaseWiringNodeClass):

    @abstractmethod
    def full_path(self, user_path: str | None) -> str:
        """The full path of the service interface"""

    def is_full_path(self, path: str) -> bool:
        return path.startswith(self.full_path('|').split('|')[0])

    def path_from_full_path(self, path: str) -> str:
        split = self.full_path('|').split('|', 1)
        return path[len(split[0]):-len(split[1])]
