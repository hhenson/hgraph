from enum import auto, IntFlag

from hgraph._runtime._global_state import GlobalState
from hgraph._types._context_type import CONTEXT
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._wiring._decorators import operator


__all__ = (
    "RecordReplayEnum",
    "RecordReplayContext",
    "register_record_replay_model",
    "record_replay_model",
    "record",
    "replay",
    "compare",
    "IN_MEMORY",
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
    """

    NONE = auto()
    RECORD = auto()
    REPLAY = auto()
    COMPARE = auto()
    REPLAY_OUTPUT = auto()
    RESET = auto()


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


def register_record_replay_model(model: str):
    """Registers the recordable model to make use of"""
    GlobalState.instance()["::record_replay_model::"] = model


IN_MEMORY = "InMemory"


def record_replay_model() -> str:
    """Get the recordable model to make use of"""
    return GlobalState.instance().get("::record_replay_model::", IN_MEMORY)


@operator
def record(ts: TIME_SERIES_TYPE, key: str, **kwargs):
    """
    Records the ts input. The recordable_context is provided containing the recordable_id as well
    as the record mode. If the mode does not contain record, then the results are not recorded.
    If the state is record, but a replay option is set, then the recording will only continue once
    the last recorded time is reached (unless the reset option is set).

    The key represents the input argument (or out for the output)
    """
    ...


@operator
def replay(key: str, tp: type[TIME_SERIES_TYPE], **kwargs) -> TIME_SERIES_TYPE:
    """
    Replay the ts using the id provided in the context.
    This will also ensure that REPLAY | COMPARE is set as the mode before attempting replay.

    The key represents the input argument (or out for the output)
    """


@operator
def compare(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, **kwargs):
    """
    Perform a comparison between two time series (when the context is set to COMPARE).
    This will write the results of the comparison to a comparison result file.
    """
