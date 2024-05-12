from dataclasses import dataclass
from typing import Generic, cast, Optional, Any

from frozendict import frozendict

from hgraph import TIME_SERIES_TYPE, sink_node, SCALAR, pull_source_node, HgScalarTypeMetaData, \
    WiringNodeClass, create_wiring_node_instance, BaseWiringNodeClass, PythonBaseNodeBuilder, NodeImpl
from hgraph.nodes.analytics._recordable_converters import record_to_table_api, get_converter_for, RecordableConverter
from hgraph.nodes.analytics._recorder_api import get_recorder_api, get_recording_label, TableReaderAPI


__all__ = ("recordable_feedback",)


@sink_node(active=("ts",), valid=("ts",))
def _recordable_feedback_sink(ts: TIME_SERIES_TYPE, ts_self: TIME_SERIES_TYPE):
    """This binds the value of ts to the _feedback source node"""
    ts_self.output.owning_node.copy_from_input(ts)


@dataclass
class RecordableFeedbackWiringPort(Generic[TIME_SERIES_TYPE]):
    _recordable_id: str
    _tp: type[TIME_SERIES_TYPE]
    _delegate: "WiringPort"
    _bound: bool = False

    def __call__(self, ts: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
        if ts is None:
            return self._delegate

        if self._bound:
            from hgraph._wiring._wiring_errors import CustomMessageWiringError
            raise CustomMessageWiringError(f"recordable_feedback is already bounded")
        self._bound = True
        _recordable_feedback_sink(ts, self._delegate)
        record_to_table_api(self._recordable_id, ts)


def recordable_feedback(
        recordable_id: str,
        tp_: type[TIME_SERIES_TYPE],
        default: SCALAR = None
) -> RecordableFeedbackWiringPort[TIME_SERIES_TYPE]:
    """
    Will track results, writing them using the recorded result.
    When the graph starts afresh, will initialise itself, with the last value prior to the current start time.
    """
    from hgraph._wiring._wiring_port import _wiring_port_for
    node_instance = _recorded_source_node(recordable_id, tp_, default)
    real_wiring_port = _wiring_port_for(node_instance.output_type, node_instance, tuple())
    return RecordableFeedbackWiringPort(_recordable_id=recordable_id, _tp=tp_, _delegate=real_wiring_port)


@pull_source_node
def _source_node_signature(recordable_id: str) -> TIME_SERIES_TYPE:
    ...


def _recorded_source_node(
        recordable_id: str,
        tp: type[TIME_SERIES_TYPE],
        default: SCALAR = None
) -> TIME_SERIES_TYPE:
    changes = {"name": "recordable_feedback"}
    inputs = {"recordable_id": recordable_id}
    signature = cast(WiringNodeClass, _source_node_signature[TIME_SERIES_TYPE: tp]).resolve_signature(recordable_id)
    if default is not None:
        default_type = HgScalarTypeMetaData.parse_value(default)
        changes["args"] = signature.args + tuple(["default"])
        changes["input_types"] = frozendict(signature.input_types | {"default": default_type})
        inputs["default"] = default
    signature = signature.copy_with(**changes)
    # Source node need to be unique, use an object instance as the fn arg to ensure uniqueness
    return create_wiring_node_instance(node=PythonRecordedSourceNodeWiringNodeClass(signature, object()),
                                       resolved_signature=signature,
                                       inputs=frozendict(inputs),
                                       rank=1)


class PythonRecordedSourceNodeWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder._node_builder import PythonLastValuePullNodeBuilder
        from hgraph import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonRecordedSourceNodeWiringNodeClass must have a time series output"
        return PythonRecordedSourceNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=None,
            output_builder=factory.make_output_builder(output_type),
            error_builder=factory.make_error_builder() if node_signature.capture_exception else None,
        )


@dataclass(frozen=True)
class PythonRecordedSourceNodeBuilder(PythonBaseNodeBuilder):

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> "PythonRecordedSourceNodeImpl":
        node = PythonRecordedSourceNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: "PythonRecordedSourceNodeImpl"):
        """Nothing to do"""


class PythonRecordedSourceNodeImpl(NodeImpl):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._delta_value: Optional[Any] = None
        if value := self.scalars.get("default"):
            self._delta_value = value
        self._recordable_id = self.scalars.get("recordable_id")

    def do_start(self):
        """Load data for the current start time"""
        start_time = self.graph.evaluation_engine_api.start_time
        reader: TableReaderAPI = get_recorder_api().get_table_reader(self._recordable_id, get_recording_label())
        reader.current_time = start_time
        # previous_time = reader.previous_available_time
        df = reader.data_frame
        if len(df) > 0:
            converter: RecordableConverter = get_converter_for(self.signature.time_series_output.py_type)
            self._delta_value = converter.convert_to_ts_value(df)
        if self._delta_value is not None:
            self.notify()

    def copy_from_input(self, output: "TimeSeriesOutput"):
        self._delta_value = output.delta_value
        self.notify_next_cycle()  # If we are copying the value now, then we expect it to be used in the next cycle

    def apply_value(self, new_value: "SCALAR"):
        self._delta_value = new_value
        self.notify_next_cycle()

    def eval(self):
        if self._delta_value is not None:
            self.output.value = self._delta_value
            self._delta_value = None
