

"""
    inspector item id is a unique identifier of an item in the graph, it can identify graphs, nodes and values in a node
    like any of its inputs, outputs or scalars

"""
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from hgraph import Graph, Node, TimeSeriesInput, TimeSeriesOutput, TimeSeriesSet
from hgraph.debug._inspector_util import format_name, base62, inspect_item, inspect_type


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
    graph: tuple[int, ...] = ()  # graph id
    node_path: tuple[str, ...] = ()  # source path to the node (in terms of graph names)
    node: int = None  # node index in the graph
    value_type: NodeValueType = None
    value_path: tuple[int | str | object, ...] = ()  # path to the value in the node, if str it is an internalised value

    _s_to_i: ClassVar[dict[object, str]] = {}  # internalised names and keys from the graph
    _i_to_s: ClassVar[dict[str, object]] = {}  # reverse lookup for internalised names and keys from the graph
    _counter: ClassVar[int] = 0 # counter for internalised names and keys from the graph

    @classmethod
    def __reset__(cls):  # use in tests only
        cls._s_to_i = {}
        cls._i_to_s = {}
        cls._counter = 0

    def __init__(self, *,
                 graph: tuple[int, ...] = (),
                 node: int = None,
                 value_type: NodeValueType = None,
                 value_path: tuple[int, ...] = ()):
        object.__setattr__(self, "graph", graph)
        object.__setattr__(self, "node", node)
        object.__setattr__(self, "value_type", value_type)
        object.__setattr__(self, "value_path", value_path)

        if self.value_type is not None:
            tp = InspectorItemType.Value
        elif self.node is not None:
            tp = InspectorItemType.Node
        else:
            tp = InspectorItemType.Graph

        object.__setattr__(self, "item_type", tp)

    @classmethod
    def _internalise(cls, s):
        if type(s) is int and s < 62**3:
            return s

        s_to_i = cls._s_to_i
        i_to_s = cls._i_to_s
        if i := s_to_i.get(s):
            return i
        else:
            cls._counter += 1
            i = f"x{base62(cls._counter)}"
            i_to_s[i] = s
            s_to_i[s] = i
            return i

    @classmethod
    def _un_internalise(cls, i):
        if type(i) is int:
            return i

        return cls._i_to_s.get(i)

    def to_str(self):
        if s := self.__dict__.get("_str"):
            return s

        graph_str = '.'.join(str(i) for i in self.graph)
        path_str = '/'.join(str(self._internalise(i)) for i in self.value_path)

        match self.item_type:
            case InspectorItemType.Graph:
                _str = f"{graph_str}"
            case InspectorItemType.Node:
                _str = f"{graph_str}:{self.node}"
            case InspectorItemType.Value:
                if path_str:
                    _str = f"{graph_str}:{self.node}/{self.value_type.value}/{path_str}"
                else:
                    _str = f"{graph_str}:{self.node}/{self.value_type.value}"

        object.__setattr__(self, "_str", _str)
        return _str

    @classmethod
    def from_str(cls, s: str):
        split_c = s.split(":")
        graph_str = split_c[0]
        rest = split_c[1] if len(split_c) > 1 else ""
        rest = rest.split("/")
        node_str = rest[0]
        rest = rest[1:]
        value_type_str = rest.pop(0) if rest else None
        path = rest

        return cls(
            graph=tuple(int(i) for i in graph_str.split(".")) if graph_str else (),
            node=int(node_str) if node_str.isdigit() else None,
            value_type=NodeValueType(value_type_str) if value_type_str else None,
            value_path=tuple(int(i) if i.isdigit() else cls._un_internalise(i) for i in path if i)
        )

    @classmethod
    def from_object(cls, o):
        if type(o) is InspectorItemId:
            return o

        from hgraph import Graph, Node, TimeSeriesInput, TimeSeriesOutput

        if isinstance(o, Graph):
            return cls(graph=o.graph_id)
        elif isinstance(o, Node):
            return cls(graph=o.graph_id, node=o.node_id)
        elif isinstance(o, TimeSeriesInput):
            path = []
            input = o
            while input.parent_input:
                key = input.parent_input.key_from_value(input)
                if key is None:
                    return None
                path.append(key)
                input = input.parent_input

            return cls(
                graph=o.owning_graph.graph_id,
                node=o.owning_node.node_ndx,
                value_type=NodeValueType.Inputs,
                value_path=tuple(i for i in reversed(path))
            )
        elif isinstance(o, TimeSeriesOutput):
            path = []
            output = o
            while output.parent_output:
                key = output.parent_output.key_from_value(output)
                if key is None:
                    return None
                path.append(key)
                output = output.parent_output

            return cls(
                graph=o.owning_graph.graph_id,
                node=o.owning_node.node_ndx,
                value_type=NodeValueType.Output,
                value_path=tuple(i for i in reversed(path))
            )
        else:
            return None

    def find_item_on_graph(self, graph):
        # graph is a graph object that matches this object's graph id
        assert graph.graph_id == self.graph

        from hgraph import PythonNestedNodeImpl

        if self.node is None:
            return graph

        node = graph.nodes[self.node]

        if self.value_type is None:
            return node

        if self.value_type == NodeValueType.Inputs:
            value = node.input
        elif self.value_type == NodeValueType.Output:
            value = node.output
        elif self.value_type == NodeValueType.Graphs:
            value = node.nested_graphs() if isinstance(node, PythonNestedNodeImpl) else {}
        elif self.value_type == NodeValueType.Scalars:
            value = node.scalars

        for i in self.value_path:
            try:
                value = inspect_item(value, i)
            except:
                if isinstance(value, (set, frozenset, TimeSeriesSet)):
                    if i in value:
                        return i
                return None

        return value

    def find_item_type(self, graph):
        # graph is a graph object that matches this object's graph id
        assert graph.graph_id == self.graph

        from hgraph import PythonNestedNodeImpl
        from hgraph import HgTypeMetaData

        if self.node is None:
            return Graph

        node = graph.nodes[self.node]

        if self.value_type is None:
            return Node

        if self.value_type == NodeValueType.Inputs:
            value = node.signature.time_series_inputs
            tp = HgTypeMetaData.parse_type(dict[str, TimeSeriesInput])
        elif self.value_type == NodeValueType.Output:
            value = node.signature.time_series_output
            tp = value
        elif self.value_type == NodeValueType.Graphs:
            value = node.nested_graphs() if isinstance(node, PythonNestedNodeImpl) else {}
            tp = HgTypeMetaData.parse_type(dict[str, Graph])
        elif self.value_type == NodeValueType.Scalars:
            value = node.scalars
            tp = value.__schema__ or HgTypeMetaData.parse_type(dict[str, object])

        for i in self.value_path:
            try:
                tp = value = inspect_type(value, i)
            except:
                return None

        if isinstance(tp, HgTypeMetaData):
            return tp
        elif isinstance(tp, type):
            return HgTypeMetaData.parse_type(tp)
        else:
            return HgTypeMetaData.parse_value(value)

    def indent(self, graph: "Graph"):
        # graph is a graph object that matches this object's graph id

        tab = "\u00A0\u00A0"
        indent = ""
        i = 0
        while i < len(self.graph):
            indent += tab
            i += 1
            if i < len(self.graph):
                indent += tab
                if i < len(self.graph) and self.graph[i] < 0:
                    i += 1
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

    def sort_key(self):
        value_type_order = {
            NodeValueType.Inputs: "X01",
            NodeValueType.Graphs: "X02",
            NodeValueType.Output: "X03",
            NodeValueType.Scalars: "X04",
        }

        sort_key = ""

        i = 0
        while i < len(self.graph):
            sort_key += base62(self.graph[i])  # node index
            i += 1
            if i < len(self.graph):
                sort_key += value_type_order[NodeValueType.Graphs]
                if i < len(self.graph) and self.graph[i] < 0:
                    sort_key += base62(-self.graph[i])
                    i += 1
                else:
                    sort_key += "000"
            else:
                sort_key += value_type_order[NodeValueType.Graphs] + "000"

        if self.node is not None:
            sort_key += base62(self.node)

        if self.value_type is not None:
            sort_key += value_type_order[self.value_type]

        sort_key += ''.join(base62(i) if type(i) is int and i < 62**3 else self._internalise(i)[1:] for i in self.value_path)
        return sort_key

    def sub_item(self, key: int | str | object, value):
        if isinstance(value, Graph):
            return InspectorItemId(graph=value.graph_id)

        if isinstance(value, Node):
            return InspectorItemId(graph=value.graph.graph_id, node=value.node_ndx)

        if isinstance(value, TimeSeriesInput) and self.value_type is None:
            assert self.graph == value.owning_graph.graph_id
            assert self.node == value.owning_node.node_ndx
            assert value.parent_input is None

            return InspectorItemId(
                graph=self.graph,
                node=self.node,
                value_type=NodeValueType.Inputs
            )

        if isinstance(value, TimeSeriesOutput) and self.value_type is None:
            assert self.graph == value.owning_graph.graph_id
            assert self.node == value.owning_node.node_ndx
            assert value.parent_output is None

            return InspectorItemId(
                graph=self.graph,
                node=self.node,
                value_type=NodeValueType.Output
            )

        if isinstance(value, NodeValueType):
            return InspectorItemId(
                graph=self.graph,
                node=self.node,
                value_type=value,
                value_path=()
            )

        assert self.value_type is not None

        return InspectorItemId(
            graph=self.graph,
            node=self.node,
            value_type=self.value_type,
            value_path=self.value_path + (key,)
        )

    def is_parent_of(self, other):
        self_str = self.sort_key()
        other_str = other.sort_key()
        return len(other_str) > len(self_str) and other_str.startswith(self_str)

    def parent_item_ids(self):
        parents = []

        i = 0
        while i < len(self.graph):
            parents.append(InspectorItemId(graph=self.graph[:i], node=self.graph[i]))
            i += 1
            if i < len(self.graph):
                parents.append(InspectorItemId(graph=self.graph[:i-1], node=self.graph[i-1], value_type=NodeValueType.Graphs))
                if i < len(self.graph) and self.graph[i] < 0:
                    i += 1
                    parents.append(InspectorItemId(graph=self.graph[:i]))
                else:
                    parents.append(InspectorItemId(graph=self.graph[:i]))

        if self.node is not None:
            parents.append(InspectorItemId(graph=self.graph, node=self.node))

        if self.value_type is not None:
            parents.append(InspectorItemId(graph=self.graph, node=self.node, value_type=self.value_type))

        for i in range(len(self.value_path)):
            parents.append(InspectorItemId(graph=self.graph, node=self.node, value_type=self.value_type, value_path=self.value_path[:i]))

        return parents[:-1]

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return f"InspectorItemId({self.to_str()})"

    def __eq__(self, other):
        return self.to_str() == other.to_str()

    def __hash__(self):
        return hash(self.to_str())