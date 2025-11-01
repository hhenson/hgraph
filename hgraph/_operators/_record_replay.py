from datetime import datetime, timedelta
from enum import auto, IntFlag
from typing import TYPE_CHECKING

from hgraph._runtime._global_state import GlobalState
from hgraph._types._scalar_types import DEFAULT, COMPOUND_SCALAR
from hgraph._types._frame_scalar_type_meta_data import Frame
from hgraph._types._time_series_types import TIME_SERIES_TYPE, OUT
from hgraph._types._ts_type import TS
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import operator

if TYPE_CHECKING:
    from hgraph import Graph, Traits

__all__ = (
    "RecordReplayEnum",
    "RecordReplayContext",
    "get_fq_recordable_id",
    "has_recordable_id_trait",
    "record_replay_model_restriction",
    "set_record_replay_model",
    "set_parent_recordable_id",
    "record_replay_model",
    "record",
    "replay",
    "replay_const",
    "compare",
    "IN_MEMORY",
    "from_data_frame",
    "to_data_frame",
)


class RecordReplayEnum(IntFlag):
    """
    RECORD
        Records the recordable components when this is set.

    REPLAY
        Replays the inputs. (If RECORD is also set, then we move to RECORD mode after replay)

    COMPARE
        Replays the inputs comparing the outputs as a form of back-testing.

    REPLAY_OUTPUT
        Replays the outputs until the last is replayed, after that the code will continue to compute the component.

    RESET
        Ignores the current state and will re-record the results.

    RECOVER
        Will recover the state of the graph using the first recorded time prior to the start-time.
        Then will continue to compute the next states.
    """

    NONE = 0
    RECORD = auto()
    REPLAY = auto()
    COMPARE = auto()
    REPLAY_OUTPUT = auto()
    RESET = auto()
    RECOVER = auto()


class RecordReplayContext:
    _instance: list["RecordReplayContext"] = []

    def __init__(self, mode: RecordReplayEnum = RecordReplayEnum.RECORD, recordable_id: str = None):
        self._mode = mode
        self._recordable_id = recordable_id

    @property
    def mode(self) -> RecordReplayEnum:
        return self._mode

    @property
    def recordable_id(self) -> str:
        return self._recordable_id

    @staticmethod
    def instance() -> "RecordReplayContext":
        if len(RecordReplayContext._instance) == 0:
            return RecordReplayContext(mode=RecordReplayEnum.NONE, recordable_id="No Context")
        return RecordReplayContext._instance[-1]

    def __enter__(self) -> "RecordReplayContext":
        self._instance.append(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._instance.pop()


def set_record_replay_model(model: str):
    """Registers the recordable model to make use of"""
    GlobalState.instance()["::record_replay_model::"] = model


IN_MEMORY = "InMemory"

RECORDABLE_ID_TRAIT = "recordable_id"


def record_replay_model() -> str:
    """Get the recordable model to make use of"""
    return GlobalState.instance().get("::record_replay_model::", IN_MEMORY)


def has_recordable_id_trait(trait: "Traits") -> bool:
    return trait.get_trait_or(RECORDABLE_ID_TRAIT, None) is not None


def get_fq_recordable_id(traits: "Traits", recordable_id: str) -> str:
    """
    resolves the recordable id by collecting the full path or recordable id's from this recordable_id to the
    outer component graph.
    """
    parent_id = traits.get_trait_or(RECORDABLE_ID_TRAIT, None) if traits else None
    if parent_id is None:
        if recordable_id is None:
            raise RuntimeError("No recordable id provided and no parent order id found")
        return recordable_id
    else:
        if recordable_id is None:
            return parent_id
        else:
            return f"{parent_id}.{recordable_id}"


def set_parent_recordable_id(graph: "Graph", recordable_id: str):
    """Set the recordable id trait on the graph"""
    graph.traits.set_traits(recordable_id=recordable_id)


def record_replay_model_restriction(model: str, check_operator: bool = False):
    """
    Ensure the operator implementation will only be available when the record model is as per ``model`` when used
    as an operator, but not if the implementation is used directly.
    """
    if check_operator:

        def restriction(m, s):
            return not s.get("is_operator", False) or record_replay_model() == model

    else:

        def restriction(m, s):
            return record_replay_model() == model

    return restriction


@operator
def record(ts: DEFAULT[TIME_SERIES_TYPE], key: str, recordable_id: str = None):
    """
    Record the ts value. The recoding is tied to the recordable_id plus the key.
    Recordings are made as delta values. A model could choose to record as full state
    so long as it is consistent.

    The key represents the input argument (or __out__ for the output)

    If not supplied, the recordable_id will be extracted from Traits for the surrounding graph.
    """
    ...


@operator
def replay(key: str, tp: type[OUT] = AUTO_RESOLVE, recordable_id: str = None) -> OUT:
    """
    Replay the ts using the id provided in the context.
    This will also ensure that REPLAY | COMPARE is set as the mode before attempting replay.

    The key represents the input argument (or out for the output)
    If not supplied, the recordable_id will be extracted from Traits for the surrounding graph.
    """


@operator
def replay_const(
    key: str, tp: type[OUT] = AUTO_RESOLVE, recordable_id: str = None, tm: datetime = None, as_of: datetime = None
) -> OUT:
    """
    Will return a const time-series of values <= start_time.
    This is used to intialise the graph prior to continued computations.
    If not supplied, the recordable_id will be extracted from Traits for the surrounding graph.

    This must be implemented as a const_fn.

    When called as a value and not as a node, the user MUST supply the recordable_id, tm and as_of values.
    """


@operator
def compare(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE):
    """
    Perform a comparison between two time series (when the context is set to COMPARE).
    This will write the results of the comparison to a comparison result file.
    """


@operator
def from_data_frame(df: Frame[COMPOUND_SCALAR], dt_col: str = "date", offset: timedelta = timedelta()) -> DEFAULT[OUT]:
    """
    Produce a time-series from a dataframe.
    The dataframe must have a date/datetime column. By default, this is expected to be called "date".
    The date can have a fixed offset applied to it (offset).

    The following schema types are supported:

    * TS[SCALAR] - Over and above the std types, we also support value_col (default: "value")
    * TSB[TS_SCHEMA]
    * TSD[SCALAR, TS[SCALAR_1]]
    """


@operator
def to_data_frame(
    ts: TIME_SERIES_TYPE, dt_col: str = "date", as_date: bool = False, include_date: bool = True
) -> TS[Frame[COMPOUND_SCALAR]]:
    """
    Provides a very simplistic mechanism to convert a time-series to a dataframe.
    For more complex conversions, use the 'to_table' tooling. This will not record key removals and represents
    the value at a point in time, not the delta value.
    """
