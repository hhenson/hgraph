from hgraph._operators import record
from hgraph._operators._record_replay import record_replay_model_restriction, replay, replay_const
from hgraph._types import TIME_SERIES_TYPE, OUT, AUTO_RESOLVE
from hgraph._wiring import graph

__all__ = ("record_to_dataframe", "replay_from_dataframe", "replay_const_from_data_frame", "RECORD_AS_DATA_FRAME")


RECORD_AS_DATA_FRAME = ":dataframe:__RecordAsDataFrame__"


@graph(overloads=record, requires=record_replay_model_restriction(RECORD_AS_DATA_FRAME))
def record_to_dataframe(ts: TIME_SERIES_TYPE, key: str, record_delta_values: bool = True, suffix: str = None):
    """
    Records data sets to data frames, not all time-series types are supported. The supported types are limited
    to those for which we can convert into tabular data structures.

    Supported types include:
    * TS[SCALAR] - When the scalar is a CompoundScalar this is only supported when the CS is flattenable
    * COMPOUND_SCALAR - When the keys are all simple scalars (i.e. not nested structures)
    * TSB[TS_SCALAR] - where the TS_SCHEMA follows the same rules as the COMPOUND SCALAR
    * TSL[TIME_SERIES_TYPE, SIZE] - Where the TIME_SERIES_TYPE is flattenable.
    * TSD[K, V] - Where V is flattenable and K is a simple primitive type.

    Since `record` is used in a generic form, the mappings to the schema follow the rules:
    * The first column is always __datetime__ and holds the current engine time.
    * When as_of recording is enabled, the second column is __as_of__.
    * TS[SCALAR] - When SCALAR is a primitive, the schema is [__datetime__: datetime, value: SCALAR]
    * COMPOUND_SCALAR - When the keys [__datetime__: datetime, k1: SCALAR, k2: SCALAR, ...]
    * TSB[TS_SCALAR] - Same as for CS
    * TSL[TIME_SERIES_TYPE, SIZE] - [__datetime__: datetime, __index__, ...] where ... is the same as for the TS type.
    * TSD[K, V] - [__datetime__: datetime, __index__: K, ...] where ... is the same as for the TS type.
    """


@graph(overloads=replay, requires=record_replay_model_restriction(RECORD_AS_DATA_FRAME))
def replay_from_dataframe(key: str, tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE, suffix: str = None) -> OUT: ...


@graph(overloads=replay_const, requires=record_replay_model_restriction(RECORD_AS_DATA_FRAME))
def replay_const_from_data_frame(key: str, tp: type[OUT] = AUTO_RESOLVE, suffix: str = None) -> OUT: ...
