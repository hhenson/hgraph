from abc import abstractmethod
from typing import Callable, TypeVar

from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature


class ServiceInterfaceNodeClass(BaseWiringNodeClass):

    def default_path(self) -> str:
        """The default path of the service interface"""
        return f"{self.signature.name}_default" if (p := self.signature.defaults.get("path")) is None else p

    @abstractmethod
    def full_path(self, user_path: str | None) -> str:
        """The full path of the service interface"""

    def is_full_path(self, path: str) -> bool:
        return path.startswith(self.full_path("|").split("|")[0]) if path else False

    def path_from_full_path(self, path: str) -> str:
        split = self.full_path("|").split("|", 1)
        l0 = len(split[0])
        return path[l0 : l0 + path[l0:].find(split[1])]

    def typed_full_path(self, path, type_map):
        if not self.is_full_path(path):
            path = self.full_path(path)

        if type_map:
            return f"{path}[{ServiceInterfaceNodeClass._resolution_dict_to_str(type_map)}]"

        return path

    @staticmethod
    def _resolution_dict_to_str(type_map: dict[TypeVar, HgTypeMetaData]):
        return (
            f"{', '.join(f'{k}:{str(type_map[k])}' for k in sorted(type_map, key=lambda x: getattr(x, '__name__', str(x))))}"
        )

    def impl_signature(self, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None) -> WiringNodeSignature:
        """
        The lowered implementation contract for this service-like interface.
        Adaptors use their public signature directly; services override this to expose the transport shape.
        """
        return self.signature

    def wire_outside_stubs(
        self, path: str, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **scalars
    ):
        """
        Wire transport stubs that need to live outside the implementation graph.
        By default there is nothing to do.
        """


def validate_signature_against_impl_signature(
    signature: WiringNodeSignature, interface: ServiceInterfaceNodeClass
) -> WiringNodeSignature:
    expected_signature = interface.impl_signature()

    missing_inputs = tuple(arg for arg in expected_signature.time_series_inputs if arg not in signature.time_series_inputs)
    if missing_inputs:
        raise CustomMessageWiringError(
            f"The implementation has missing inputs compared to the lowered '{interface.signature.name}' signature: "
            f"{', '.join(missing_inputs)}"
        )

    unexpected_inputs = tuple(arg for arg in signature.time_series_inputs if arg not in expected_signature.time_series_inputs)
    if unexpected_inputs:
        raise CustomMessageWiringError(
            f"The implementation has unexpected time-series inputs for '{interface.signature.name}': "
            f"{', '.join(unexpected_inputs)}"
        )

    for arg, expected_tp in expected_signature.time_series_inputs.items():
        actual_tp = signature.time_series_inputs[arg]
        if not actual_tp.matches(expected_tp):
            raise CustomMessageWiringError(
                f"The implementation input {arg}: {actual_tp} does not match the lowered service signature {expected_tp}"
            )

    expected_output = expected_signature.output_type
    actual_output = signature.output_type
    if expected_output is None:
        if actual_output is not None:
            raise CustomMessageWiringError(
                f"The implementation should not return an output for '{interface.signature.name}'"
            )
    elif actual_output is None or not actual_output.dereference().matches(expected_output.dereference()):
        raise CustomMessageWiringError(
            f"The implementation output {actual_output} does not match the lowered service signature {expected_output}"
        )

    return signature
