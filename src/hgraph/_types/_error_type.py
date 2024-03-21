from dataclasses import dataclass
from typing import Mapping, TYPE_CHECKING

from hgraph._types._ref_type import TimeSeriesReference
from hgraph._types._scalar_types import CompoundScalar

if TYPE_CHECKING:
    from hgraph._runtime._node import Node

__all__ = ("NodeError", "NodeException", "raise_error")


@dataclass(frozen=True)
class BacktraceSignature(CompoundScalar):
    name: str
    args: tuple[str, ...]


@dataclass(frozen=True)
class BackTrace:
    signature: BacktraceSignature
    active_inputs: Mapping[str, "BackTrace"]
    input_values: Mapping[str, str]

    def _arg_str(self, arg_name: str) -> str:
        if self.active_inputs and arg_name in self.active_inputs:
            return f"*{arg_name}*" + (f"={self.input_values[arg_name]}" if (
                        self.input_values and arg_name in self.input_values) else '')
        if self.input_values and arg_name in self.input_values:
            return f"{arg_name}={self.input_values[arg_name]}"
        else:
            return arg_name

    def _level_str(self, level: int = 0) -> str:
        if self.signature is None:
            return ""
        indent = ' ' * 2 * level
        args = ", ".join(self._arg_str(arg) for arg in self.signature.args if not arg.startswith("_"))
        s = f"{indent}{self.signature.name}({args})\n"
        s += "\n".join(f"{indent}{arg}:\n{value._level_str(level + 1)}" for arg, value in
                       (self.active_inputs.items() if self.active_inputs else tuple()))
        return s

    def __str__(self):
        return self._level_str()

    @staticmethod
    def capture_back_trace(node: "Node", capture_values: bool = False, depth: int = 4) -> "BackTrace":
        signature = BacktraceSignature(node.signature.name, node.signature.args) if node else None
        if depth > 0:
            active_inputs = {}
            input_values = {}
            for input_name, input in node.inputs.items() if node else tuple():
                BackTrace.capture_input(active_inputs, input, input_name, capture_values, depth)
                if capture_values:
                    input_values[input_name] = (v := str(input.value))[0:256] + ("..." if len(v) > 255 else "")
            return BackTrace(signature=signature, active_inputs=active_inputs,
                             input_values=input_values if capture_values else None)
        else:
            return BackTrace(signature=signature, active_inputs=None, input_values=None)

    @staticmethod
    def capture_input(active_inputs, input, input_name, capture_values, depth):
        if input.modified:
            node = None
            if input.bound:
                if input.has_peer:
                    active_inputs[input_name] = BackTrace.capture_back_trace(input.output.owning_node,
                                                                             capture_values, depth - 1)
                else:
                    for n, i in input.items():
                        BackTrace.capture_input(active_inputs, i, f"{input_name}[{n}]", capture_values, depth - 1)
            elif isinstance(input.value, TimeSeriesReference) and input.value.output:
                active_inputs[input_name] = BackTrace.capture_back_trace(input.value.output.owning_node, capture_values,
                                                                         depth - 1)


@dataclass(frozen=True)
class NodeError(CompoundScalar):
    signature_name: str
    label: str
    wiring_path: str
    error_msg: str
    stack_trace: str
    activation_back_trace: str
    additional_context: str = None

    def __str__(self):
        s = (f"{self.signature_name}" +
             (f"labelled {self.label}" if self.label else "") +
             (f" at {self.wiring_path}" if self.wiring_path else "") +
             f"{' :: ' + self.additional_context if self.additional_context else ''}\n"
             f"NodeError: {self.error_msg}\nStack trace:\n{self.stack_trace}"
             f"\nActivation Back Trace:\n{self.activation_back_trace}")
        return s

    @classmethod
    def capture_error(cls, exception: Exception, node: "Node", message: str = None):
        if isinstance(exception, NodeError):
            return exception

        import traceback
        back_trace = BackTrace.capture_back_trace(node, capture_values=node.signature.capture_values,
                                                  depth=node.signature.trace_back_depth)
        error = cls(
            signature_name=node.signature.signature,
            label=node.signature.label,
            wiring_path=node.signature.wiring_path_name,
            error_msg=str(exception),
            stack_trace="\n".join(traceback.format_exc().splitlines()),
            activation_back_trace=str(back_trace),
            additional_context=message
        )
        return error


class NodeException(NodeError, Exception):
    ...


def raise_error(msg: str):
    raise RuntimeError(msg)