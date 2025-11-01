from dataclasses import dataclass
from datetime import datetime
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
    wiring_path_name: str
    runtime_path_name: str
    node_id: str


@dataclass(frozen=True)
class BackTrace:
    signature: BacktraceSignature
    active_inputs: Mapping[str, "BackTrace"]
    input_short_values: Mapping[str, str] = None
    input_delta_values: Mapping[str, str] = None
    input_last_modified_time: Mapping[str, datetime] = None
    input_values: Mapping[str, str] = None

    def _arg_str(self, arg_name: str) -> str:
        if self.active_inputs and arg_name in self.active_inputs:
            return f"*{arg_name}*" + (
                f"={self.input_short_values[arg_name]}"
                if (self.input_short_values and arg_name in self.input_short_values)
                else ""
            )
        if self.input_values and arg_name in self.input_values:
            return f"{arg_name}={self.input_short_values[arg_name]}"
        else:
            return arg_name

    def _level_str(self, level: int = 0) -> str:
        if self.signature is None:
            return ""
        indent = " " * 2 * level
        args = ", ".join(self._arg_str(arg) for arg in self.signature.args if not arg.startswith("_"))
        s = (
            f"{indent}{self.signature.runtime_path_name}<{', '.join(str(i) for i in self.signature.node_id)}>:"
            f" {self.signature.name}({args})\n"
        )
        arg_strs = []
        if self.input_values:
            for arg, value in self.input_values.items():
                if arg in self.active_inputs:
                    arg_str = f"{indent}*{arg}*:"
                else:
                    arg_str = f"{indent}{arg}:"

                dv = self.input_delta_values.get(arg)
                if dv is not None:
                    arg_str += f"\n{indent}  delta_value={dv}"
                if value != dv:
                    arg_str += f"\n{indent}  value={value}"

                arg_str += f"\n{indent}  last modified={self.input_last_modified_time.get(arg)}"

                if arg in self.active_inputs:
                    arg_str += "\n" + self.active_inputs[arg]._level_str(level + 1)

                arg_strs.append(arg_str)
        else:
            for arg, value in self.active_inputs.items() if self.active_inputs else tuple():
                arg_strs.append(f"{indent}{arg}:\n{value._level_str(level + 1)}")
        return s + "\n".join(arg_strs)

    def __str__(self):
        return self._level_str()

    @staticmethod
    def runtime_path_name(node: "Node", use_label: bool = True) -> str:
        sig = node.signature
        suffix = (sig.label or sig.name) if use_label else sig.name
        parent_node = node.graph.parent_node
        if parent_node:
            p_l = BackTrace.runtime_path_name(parent_node)
            p_n = BackTrace.runtime_path_name(parent_node, use_label=False)
            p_n = _remove_indices(p_n)
            return f"{p_l}[{node.graph.label}].{sig.wiring_path_name.replace(p_n, '')}.{suffix}".replace("..", ".")
        else:
            return f"{sig.wiring_path_name}.{suffix}"

    @staticmethod
    def capture_back_trace(node: "Node", capture_values: bool = False, depth: int = 4) -> "BackTrace":
        signature = (
            BacktraceSignature(
                name=node.signature.name,
                args=node.signature.args,
                wiring_path_name=node.signature.wiring_path_name,
                runtime_path_name=BackTrace.runtime_path_name(node),
                node_id=node.node_id,
            )
            if node
            else None
        )

        if depth > 0:
            active_inputs = {}
            input_values = {}
            input_delta_values = {}
            input_short_values = {}
            input_last_modified_time = {}
            for input_name, input in node.inputs.items() if node else tuple():
                BackTrace.capture_input(active_inputs, input, input_name, capture_values, depth)
                if capture_values:
                    input_short_values[input_name] = (v := str(input.value).split("\n")[0])[0:32] + (
                        "..." if len(v) > 32 else ""
                    )
                    input_delta_values[input_name] = str(input.delta_value)
                    input_values[input_name] = v
                    input_last_modified_time[input_name] = input.last_modified_time
            return BackTrace(
                signature=signature,
                active_inputs=active_inputs,
                input_short_values=input_short_values,
                input_delta_values=input_delta_values,
                input_values=input_values,
                input_last_modified_time=input_last_modified_time,
            )
        else:
            return BackTrace(signature=signature, active_inputs=None)

    @staticmethod
    def capture_input(active_inputs, input, input_name, capture_values, depth):
        if input.modified:
            node = None
            if input.bound:
                if input.has_peer:
                    active_inputs[input_name] = BackTrace.capture_back_trace(
                        input.output.owning_node, capture_values, depth - 1
                    )
                else:
                    for n, i in input.items():
                        BackTrace.capture_input(active_inputs, i, f"{input_name}[{n}]", capture_values, depth - 1)
            elif TimeSeriesReference.is_instance(input.value) and input.value.output:
                active_inputs[input_name] = BackTrace.capture_back_trace(
                    input.value.output.owning_node, capture_values, depth - 1
                )


def _remove_indices(s):
    # Remove [xyz] and [123] from "a[xyz].b.c.d[123].e.f.g" leaving "a.b.c.d.e.f.g"
    while "[" in s:
        pieces = s.split("[", maxsplit=1)
        prior = pieces[0]
        pieces = pieces[1].split("]", maxsplit=1)
        s = f"{prior}{pieces[-1]}"
    return s


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
        s = (
            f"{self.signature_name}"
            + (f"labelled {self.label}" if self.label else "")
            + (f" at {self.wiring_path}" if self.wiring_path else "")
            + f"{' :: ' + self.additional_context if self.additional_context else ''}\n"
            f"NodeError: {self.error_msg}\nStack trace:\n{self.stack_trace}"
            f"\nActivation Back Trace:\n{self.activation_back_trace}"
        )
        return s

    @classmethod
    def capture_error(cls, exception: Exception, node: "Node", message: str = None):
        if isinstance(exception, NodeError):
            return exception

        import traceback

        back_trace = BackTrace.capture_back_trace(
            node, capture_values=node.signature.capture_values, depth=node.signature.trace_back_depth
        )
        error = cls(
            signature_name=node.signature.signature,
            label=node.signature.label,
            wiring_path=node.signature.wiring_path_name,
            error_msg=str(exception),
            stack_trace="\n".join(traceback.format_exc().splitlines()),
            activation_back_trace=str(back_trace),
            additional_context=message,
        )
        return error


class NodeException(NodeError, Exception): ...


def raise_error(msg: str):
    raise RuntimeError(msg)
