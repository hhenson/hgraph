from abc import abstractmethod
from typing import TypeVar

from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass


class ServiceInterfaceNodeClass(BaseWiringNodeClass):

    @abstractmethod
    def full_path(self, user_path: str | None) -> str:
        """The full path of the service interface"""

    def is_full_path(self, path: str) -> bool:
        return path.startswith(self.full_path('|').split('|')[0]) if path else False

    def path_from_full_path(self, path: str) -> str:
        split = self.full_path('|').split('|', 1)
        return path[len(split[0]):-len(split[1])]

    def typed_full_path(self, path, type_map):
        if not self.is_full_path(path):
            path = self.full_path(path)

        if type_map:
            return f"{path}[{ServiceInterfaceNodeClass._resolution_dict_to_str(type_map)}]"

        return path

    @staticmethod
    def _resolution_dict_to_str(type_map: dict[TypeVar, HgTypeMetaData]):
        return f"{', '.join(f'{k}:{str(type_map[k])}' for k in sorted(type_map, key=lambda x: x.__name__))}"
