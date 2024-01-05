from dataclasses import dataclass
from typing import Mapping, TYPE_CHECKING

from hgraph._types._ref_type import TimeSeriesReference
from hgraph._types._scalar_types import CompoundScalar

if TYPE_CHECKING:
    from hgraph._runtime._node import Node

__all__ = ("NodeError",)


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
                if input.modified:
                    node = None
                    if input.bound:
                        node = input.output.owning_node
                    elif isinstance(input.value, TimeSeriesReference) and input.value.output:
                        node = input.value.output.owning_node
                    active_inputs[input_name] = BackTrace.capture_back_trace(node, capture_values, depth - 1)
                if capture_values:
                    input_values[input_name] = (v := str(input.value))[0:256] + ("..." if len(v) > 255 else "")
            return BackTrace(signature=signature, active_inputs=active_inputs,
                             input_values=input_values if capture_values else None)
        else:
            return BackTrace(signature=signature, active_inputs=None, input_values=None)


@dataclass(frozen=True)
class NodeError(CompoundScalar):
    signature_name: str
    error_msg: str
    stack_trace: str
    activation_back_trace: str
    additional_context: str = None

    def __str__(self):
        s = (f"{self.signature_name}"
             f"{' :: ' + self.additional_context if self.additional_context else ''}\n"
             f"NodeError: {self.error_msg}\nStack trace:\n{self.stack_trace}"
             f"\nActivation Back Trace:\n{self.activation_back_trace}")
        return s

    @staticmethod
    def capture_error(exception: Exception, node: "Node", message: str = None):
        import traceback
        back_trace = BackTrace.capture_back_trace(node, capture_values=node.signature.capture_values,
                                                  depth=node.signature.trace_back_depth)
        error = NodeError(
            signature_name=node.signature.signature,
            error_msg=str(exception),
            stack_trace="\n".join(traceback.format_exc().splitlines()),
            activation_back_trace=str(back_trace),
        )
        return error
