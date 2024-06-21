import typing
from dataclasses import dataclass
from functools import cached_property
from typing import Mapping, Generic

from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._types._scalar_types import SCALAR, ZERO
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
from hgraph._types._tsb_type import TimeSeriesSchema
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import KEY_SET_ID
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._types._typing_utils import nth
from hgraph._wiring._wiring_context import WIRING_CONTEXT
from hgraph._wiring._wiring_errors import CustomMessageWiringError

if typing.TYPE_CHECKING:
    from hgraph import WiringNodeInstance

__all__ = (
    "WiringPort",
    "ErrorWiringPort",
    "TSDWiringPort",
    "TSDREFWiringPort",
    "DelayedBindingWiringPort",
    "TSBWiringPort",
    "TSBREFWiringPort",
    "TSLWiringPort",
    "TSLREFWiringPort",
)


def _wiring_port_for(tp: HgTypeMetaData, node_instance: "WiringNodeInstance", path: [int, ...]) -> "WiringPort":
    return {
        HgTSDTypeMetaData: lambda: TSDWiringPort(node_instance, path),
        HgTSBTypeMetaData: lambda: TSBWiringPort(node_instance, path),
        HgTSLTypeMetaData: lambda: TSLWiringPort(node_instance, path),
        HgREFTypeMetaData: lambda: {
            HgTSDTypeMetaData: lambda: TSDREFWiringPort(node_instance, path),
            HgTSBTypeMetaData: lambda: TSBREFWiringPort(node_instance, path),
            HgTSLTypeMetaData: lambda: TSLREFWiringPort(node_instance, path),
        }.get(type(tp.value_tp), lambda: WiringPort(node_instance, path))(),
    }.get(type(tp), lambda: WiringPort(node_instance, path))()


@dataclass(frozen=True, eq=False, unsafe_hash=False)
class WiringPort:
    """
    A wiring port is the abstraction that describes the src of an edge in a wiring graph. This source is used to
    connect to a destination node in the graph, typically an input in the graph.
    The port consists of a reference to a node instance, this is the node in the graph to connect to, and a path, this
    is the selector used to identify to which time-series owned by the node this portion of the edge refers to.

    For a simple time-series (e.g. TS[SCALAR]), the path is an empty tuple. For more complex time-series containers,
    the path can be any valid SCALAR value that makes sense in the context of the container.
    The builder will ultimately walk the path calling __getitem__ the time-series until the path is completed.

    For example, node.output[p1][p2][p3] for a path of (p1, p2, p3).
    """

    node_instance: "WiringNodeInstance"
    path: tuple[SCALAR, ...] = tuple()  # The path from out () to the time-series to be bound.

    @property
    def has_peer(self) -> bool:
        from hgraph._wiring._wiring_node_class._stub_wiring_node_class import NonPeeredWiringNodeClass

        return not isinstance(self.node_instance.node, NonPeeredWiringNodeClass)

    def edges_for(
        self, node_map: Mapping["WiringNodeInstance", int], dst_node_ndx: int, dst_path: tuple[SCALAR, ...]
    ) -> set["Edge"]:
        """Return the edges required to bind this output to the dst_node"""
        assert (
            self.has_peer
        ), "Can not bind a non-peered node, the WiringPort must be sub-classed and override this method"

        from hgraph._builder._graph_builder import Edge

        return {Edge(node_map[self.node_instance], self.path, dst_node_ndx, dst_path)}

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        output_type = self.node_instance.output_type
        for p in self.path:
            # This is the path within a TSB
            output_type = output_type[p]
        return output_type

    def __error__(self, trace_back_depth: int = 1, capture_values: bool = False) -> "WiringPort":
        if self.path == tuple():
            self.node_instance.mark_error_handler_registered(trace_back_depth, capture_values)
            return ErrorWiringPort(
                self.node_instance,
                tuple(
                    [
                        -1,
                    ]
                ),
            )
        else:
            raise CustomMessageWiringError("Wiring ports are only accessible on the main return value")


@dataclass(frozen=True)
class ErrorWiringPort(WiringPort):

    def __error__(self, *args, **kwargs) -> "WiringPort":
        raise CustomMessageWiringError("This is the error wiring Port")

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.node_instance.error_output_type


@dataclass(frozen=True)
class DelayedBindingWiringPort(WiringPort):
    """
    A wiring port that is not yet bound to a node. This is used in the graph builder to create a placeholder for
    a wiring port that will be bound later.
    """

    node_instance: "WiringNodeInstance" = None
    output_type: HgTimeSeriesTypeMetaData = None

    def bind(self, wiring_port: WiringPort):
        if self.output_type != wiring_port.output_type:
            raise CustomMessageWiringError(
                "The output type of the delayed binding port does not match the output type " "of the port being bound"
            )

        object.__setattr__(self, "node_instance", wiring_port.node_instance)
        object.__setattr__(self, "path", wiring_port.path)


@dataclass(frozen=True)
class TSDWiringPort(WiringPort, Generic[SCALAR, TIME_SERIES_TYPE]):

    @property
    def key_set(self) -> TSS[SCALAR]:
        return WiringPort(self.node_instance, self.path + (KEY_SET_ID,))

    def __getitem__(self, key):
        from hgraph import getitem_

        return getitem_(self, key)

    def reduce(self, fn, zero=ZERO):
        from hgraph import reduce

        return reduce(fn, self, zero)


@dataclass(frozen=True)
class TSDREFWiringPort(WiringPort, Generic[SCALAR, TIME_SERIES_TYPE]):

    @property
    def key_set(self) -> TSS[SCALAR]:
        from hgraph import keys_

        return keys_(self)

    def __getitem__(self, key):
        from hgraph import getitem_
        return getitem_(self, key)

    def reduce(self, fn, zero=ZERO):
        from hgraph import reduce

        return reduce(fn, self, zero)


@dataclass(frozen=True)
class TSBWiringPort(WiringPort):

    @cached_property
    def __schema__(self) -> "TimeSeriesSchema":
        return self.output_type.bundle_schema_tp.py_type

    @property
    def as_schema(self):
        """Support the as_schema syntax"""
        return self

    def __getattr__(self, item):
        return self._wiring_port_for(item)

    def _wiring_port_for(self, item):
        """Support the path selection using property names"""
        schema: TimeSeriesSchema = self.__schema__
        if type(item) is str:
            arg = item
            ndx = schema._schema_index_of(item)
        elif type(item) is int:
            ndx = item
            arg = nth(schema.__meta_data_schema__.keys(), item)
        else:
            raise AttributeError(f"'{item}' is not typeof str or int")
        tp = schema.__meta_data_schema__[arg]
        if self.has_peer:
            path = self.path + (ndx,)
            node_instance = self.node_instance
        else:
            input_wiring_port = self.node_instance.inputs[arg]
            node_instance = input_wiring_port.node_instance
            tp = input_wiring_port.output_type
            path = input_wiring_port.path
        return _wiring_port_for(tp, node_instance, path)

    def __getitem__(self, item):
        return self._wiring_port_for(item)

    def as_dict(self):
        return {k: self[k] for k in self.__schema__.__meta_data_schema__.keys()}

    def as_scalar_ts(self):
        if self.__schema__.scalar_type() is None:
            raise CustomMessageWiringError("The schema does not have a scalar type")

        from hgraph import convert, CompoundScalar, TS

        return convert[TS[CompoundScalar]](self)

    def edges_for(
        self, node_map: Mapping["WiringNodeInstance", int], dst_node_ndx: int, dst_path: tuple[SCALAR, ...]
    ) -> set["Edge"]:
        edges = set()
        if self.has_peer:
            from hgraph._builder._graph_builder import Edge

            edges.add(Edge(node_map[self.node_instance], self.path, dst_node_ndx, dst_path))
        else:
            for ndx, arg in enumerate(self.__schema__.__meta_data_schema__):
                wiring_port = self._wiring_port_for(arg)
                edges.update(wiring_port.edges_for(node_map, dst_node_ndx, dst_path + (ndx,)))
        return edges

    def copy_with(self, **kwargs):
        """
        Creates a new instance of a wiring time bundle using the values of this instance combined / overridden from
        the kwargs provided. Can be used to clone a runtime instance of a bundle as well.
        """
        self.output_type.py_type._validate_kwargs(self.__schema__, **kwargs)
        tsb = self.output_type.py_type.from_ts(**(self.as_dict() | kwargs))
        return tsb


@dataclass(frozen=True)
class TSBREFWiringPort(WiringPort):

    @cached_property
    def __schema__(self) -> "TimeSeriesSchema":
        return self.output_type.value_tp.bundle_schema_tp.py_type

    @property
    def as_schema(self):
        """Support the as_schema syntax"""
        return self

    def as_scalar_ts(self):
        if self.__schema__.scalar_type() is None:
            raise CustomMessageWiringError("The schema does not have a scalar type")

        from hgraph import convert, CompoundScalar, TS

        return convert[TS[CompoundScalar]](self)

    def __getattr__(self, item):
        from hgraph._operators import getitem_
        return getitem_(self, item)

    def __getitem__(self, item):
        from hgraph._operators import getitem_
        return getitem_(self, item)

    def as_dict(self):
        return {k: self[k] for k in self.__schema__.__meta_data_schema__.keys()}

    def copy_with(self, **kwargs):
        """
        Creates a new instance of a wiring time bundle using the values of this instance combined / overridden from
        the kwargs provided. Can be used to clone a runtime instance of a bundle as well.
        """
        self.output_type.value_tp.py_type._validate_kwargs(self.__schema__, **kwargs)
        tsb = self.output_type.value_tp.py_type.from_ts(**(self.as_dict() | kwargs))
        return tsb


@dataclass(frozen=True)
class TSLWiringPort(WiringPort):

    def __len__(self):
        return typing.cast(HgTSLTypeMetaData, self.output_type).size.SIZE

    def values(self):
        return (self[i] for i in range(len(self)))

    def items(self):
        return ((i, self[i]) for i in range(len(self)))

    def keys(self):
        return range(len(self))

    def __getitem__(self, item):
        """Return the wiring port for an individual TSL element"""
        if isinstance(item, WiringPort):
            from hgraph import getitem_

            return getitem_(self, item)

        output_type: HgTSLTypeMetaData = self.output_type
        tp_ = output_type.value_tp
        size_ = output_type.size
        if not size_.FIXED_SIZE:
            raise CustomMessageWiringError(
                "Currently we are unable to select a time-series element from an unbounded TSL"
            )
        elif item >= size_.SIZE:
            # Unfortunately, zip seems to depend on an IndexError being raised, so try and provide
            # as much useful context in the error message as possible
            msg = f"When resolving '{WIRING_CONTEXT.signature}' \n"
            f"Trying to select an element from a TSL that is out of bounds: {item} >= {size_.SIZE}"
            raise IndexError(msg)

        if item < 0:
            # TODO: Temp fix for negative indices. This needs a different solution when we have non-fixed TSL impl
            item = size_.SIZE + item

        if self.has_peer:
            path = self.path + (item,)
            node_instance = self.node_instance
        else:
            args = self.node_instance.resolved_signature.args
            ts_args = self.node_instance.resolved_signature.time_series_args
            arg = nth(filter(lambda k_: k_ in ts_args, args), item)
            input_wiring_port = self.node_instance.inputs[arg]
            node_instance = input_wiring_port.node_instance
            path = input_wiring_port.path
        return _wiring_port_for(tp_, node_instance, path)

    def edges_for(
        self, node_map: Mapping["WiringNodeInstance", int], dst_node_ndx: int, dst_path: tuple[SCALAR, ...]
    ) -> set["Edge"]:
        edges = set()
        if self.has_peer:
            from hgraph._builder._graph_builder import Edge

            edges.add(Edge(node_map[self.node_instance], self.path, dst_node_ndx, dst_path))
        else:
            # This should work as we don't support unbounded TSLs as non-peered nodes at the moment.
            for ndx in range(self.output_type.size.SIZE):
                wiring_port = self[ndx]
                edges.update(wiring_port.edges_for(node_map, dst_node_ndx, dst_path + (ndx,)))
        return edges


@dataclass(frozen=True)
class TSLREFWiringPort(WiringPort):

    def __len__(self):
        return typing.cast(HgTSLTypeMetaData, self.output_type.value_tp).size.SIZE

    def values(self):
        return (self[i] for i in range(len(self)))

    def items(self):
        return ((i, self[i]) for i in range(len(self)))

    def keys(self):
        return range(len(self))

    def __getitem__(self, item):
        from hgraph import getitem_

        return getitem_(self, item)
