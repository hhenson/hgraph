"""Stable identifiers used by the interactive Python inspector.

The spelling matches released hgraph.  Resolution is performed against owned
``GraphDiagnostics`` snapshots, so these identifiers never retain runtime
graph, node, or time-series objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import ClassVar


_SYMBOLS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def _base62(value: int) -> str:
    if value < 0 or value >= 62**3:
        raise ValueError("inspector identifiers require values in [0, 62**3)")
    return (
        _SYMBOLS[value // 3844]
        + _SYMBOLS[value // 62 % 62]
        + _SYMBOLS[value % 62]
    )


class InspectorItemType(Enum):
    Graph = "graph"
    Node = "node"
    Value = "value"


class NodeValueType(Enum):
    Inputs = "INPUTS"
    Output = "OUTPUT"
    Graphs = "GRAPHS"
    Scalars = "SCALARS"


@dataclass(frozen=True, init=False, kw_only=True)
class InspectorItemId:
    item_type: InspectorItemType
    graph: tuple[int, ...] = ()
    node_path: tuple[str, ...] = ()
    node: int | None = None
    value_type: NodeValueType | None = None
    value_path: tuple[object, ...] = ()

    _s_to_i: ClassVar[dict[object, str]] = {}
    _i_to_s: ClassVar[dict[str, object]] = {}
    _counter: ClassVar[int] = 0

    def __init__(
        self,
        *,
        graph: tuple[int, ...] = (),
        node: int | None = None,
        value_type: NodeValueType | None = None,
        value_path: tuple[object, ...] = (),
    ):
        object.__setattr__(self, "graph", tuple(graph))
        object.__setattr__(self, "node", node)
        object.__setattr__(self, "value_type", value_type)
        object.__setattr__(self, "value_path", tuple(value_path))
        object.__setattr__(
            self,
            "item_type",
            (
                InspectorItemType.Value
                if value_type is not None
                else InspectorItemType.Node
                if node is not None
                else InspectorItemType.Graph
            ),
        )

    @classmethod
    def __reset__(cls):
        cls._s_to_i = {}
        cls._i_to_s = {}
        cls._counter = 0

    @classmethod
    def _internalise(cls, value):
        if type(value) is int and 0 <= value < 62**3:
            return value
        if value in cls._s_to_i:
            return cls._s_to_i[value]
        cls._counter += 1
        encoded = f"x{_base62(cls._counter)}"
        cls._s_to_i[value] = encoded
        cls._i_to_s[encoded] = value
        return encoded

    @classmethod
    def _un_internalise(cls, value):
        return value if type(value) is int else cls._i_to_s.get(value)

    def to_str(self) -> str:
        graph = ".".join(str(i) for i in self.graph)
        path = "/".join(str(self._internalise(i)) for i in self.value_path)
        if self.item_type is InspectorItemType.Graph:
            return graph
        if self.item_type is InspectorItemType.Node:
            return f"{graph}:{self.node}"
        suffix = f"/{path}" if path else ""
        return f"{graph}:{self.node}/{self.value_type.value}{suffix}"

    @classmethod
    def from_str(cls, value: str):
        graph_text, separator, remainder = value.partition(":")
        graph = tuple(int(i) for i in graph_text.split(".") if i)
        if not separator:
            return cls(graph=graph)
        parts = remainder.split("/")
        node = int(parts[0]) if parts[0].isdigit() else None
        value_type = NodeValueType(parts[1]) if len(parts) > 1 and parts[1] else None
        path = tuple(
            int(item) if item.isdigit() else cls._un_internalise(item)
            for item in parts[2:]
            if item
        )
        return cls(graph=graph, node=node, value_type=value_type, value_path=path)

    @classmethod
    def from_object(cls, value):
        if isinstance(value, cls):
            return value
        graph_id = getattr(value, "graph_id", None)
        node_index = getattr(value, "node_ndx", None)
        if graph_id is not None and node_index is None:
            return cls(graph=tuple(graph_id))
        if graph_id is not None and node_index is not None:
            return cls(graph=tuple(graph_id), node=int(node_index))

        owner_graph = getattr(value, "owning_graph", None)
        owner_node = getattr(value, "owning_node", None)
        if owner_graph is None or owner_node is None:
            return None
        is_input = "Input" in type(value).__name__
        path = []
        current = value
        parent_name = "parent_input" if is_input else "parent_output"
        while (parent := getattr(current, parent_name, None)) is not None:
            key_from_value = getattr(parent, "key_from_value", None)
            key = key_from_value(current) if callable(key_from_value) else None
            if key is None:
                path.clear()
            else:
                path.append(key)
            current = parent
        node_index = getattr(owner_node, "node_ndx", None)
        if node_index is None:
            node_index = owner_node.node_id[-1]
        return cls(
            graph=tuple(owner_graph.graph_id),
            node=int(node_index),
            value_type=NodeValueType.Inputs if is_input else NodeValueType.Output,
            value_path=tuple(reversed(path)),
        )

    def indent(self, graph=None):
        del graph  # retained for the released helper signature
        tab = "\u00a0\u00a0"
        indent = ""
        index = 0
        while index < len(self.graph):
            indent += tab
            index += 1
            if index < len(self.graph):
                indent += tab
                if self.graph[index] < 0:
                    index += 1
                    indent += tab
                else:
                    indent += tab
            else:
                indent += tab + tab
        if self.node is None:
            return indent
        if self.value_type is None:
            return indent + tab
        return indent + tab * (2 + len(self.value_path))

    def sub_item(self, key, value):
        resolved = self.from_object(value)
        if resolved is not None:
            if resolved.item_type in {
                InspectorItemType.Graph,
                InspectorItemType.Node,
            }:
                return resolved
            if self.value_type is None:
                parent_name = (
                    "parent_input"
                    if resolved.value_type is NodeValueType.Inputs
                    else "parent_output"
                )
                if getattr(value, parent_name, None) is not None:
                    raise ValueError(
                        "only a node's root input or output can be a direct child"
                    )
                return InspectorItemId(
                    graph=self.graph,
                    node=self.node,
                    value_type=resolved.value_type,
                )
        if isinstance(value, NodeValueType):
            return InspectorItemId(
                graph=self.graph,
                node=self.node,
                value_type=value,
            )
        if self.value_type is None:
            raise ValueError("a graph or node item requires a NodeValueType child")
        return InspectorItemId(
            graph=self.graph,
            node=self.node,
            value_type=self.value_type,
            value_path=self.value_path + (key,),
        )

    def sort_key(self) -> str:
        order = {
            NodeValueType.Inputs: "X01",
            NodeValueType.Graphs: "X02",
            NodeValueType.Output: "X03",
            NodeValueType.Scalars: "X04",
        }
        result = ""
        index = 0
        while index < len(self.graph):
            graph_part = self.graph[index]
            result += _base62(abs(graph_part))
            index += 1
            if index < len(self.graph):
                result += order[NodeValueType.Graphs]
                if self.graph[index] < 0:
                    result += _base62(-self.graph[index])
                    index += 1
                else:
                    result += "000"
            else:
                result += order[NodeValueType.Graphs] + "000"
        if self.node is not None:
            result += _base62(self.node)
        if self.value_type is not None:
            result += order[self.value_type]
        result += "".join(
            _base62(item)
            if type(item) is int and 0 <= item < 62**3
            else self._internalise(item)[1:]
            for item in self.value_path
        )
        return result

    def is_parent_of(self, other: "InspectorItemId") -> bool:
        own = self.sort_key()
        candidate = other.sort_key()
        return len(candidate) > len(own) and candidate.startswith(own)

    def parent_item_ids(self):
        parents = []
        index = 0
        while index < len(self.graph):
            parents.append(
                InspectorItemId(
                    graph=self.graph[:index],
                    node=self.graph[index],
                )
            )
            index += 1
            if index < len(self.graph):
                parents.append(
                    InspectorItemId(
                        graph=self.graph[: index - 1],
                        node=self.graph[index - 1],
                        value_type=NodeValueType.Graphs,
                    )
                )
                if self.graph[index] < 0:
                    index += 1
                parents.append(InspectorItemId(graph=self.graph[:index]))
        if self.node is not None:
            parents.append(InspectorItemId(graph=self.graph, node=self.node))
        if self.value_type is not None:
            parents.append(
                InspectorItemId(
                    graph=self.graph,
                    node=self.node,
                    value_type=self.value_type,
                )
            )
        for length in range(len(self.value_path)):
            parents.append(
                InspectorItemId(
                    graph=self.graph,
                    node=self.node,
                    value_type=self.value_type,
                    value_path=self.value_path[:length],
                )
            )
        return parents

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return f"InspectorItemId({self.to_str()})"
