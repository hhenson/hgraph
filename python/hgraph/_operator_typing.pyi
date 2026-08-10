"""Generated typing declarations for operators exposed lazily by ``hgraph``.

Regenerate with ``tools/api_inventory.py``; runtime dispatch remains owned
by the native operator registry."""

from typing import Any, Protocol, Self

from ._wiring import WiringPort

class _OperatorShape000(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape001(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape002(Protocol):
    def __call__(self, a: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape003(Protocol):
    def __call__(self, a: Any = ..., q: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape004(Protocol):
    def __call__(self, cases: Any = ..., ts: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape005(Protocol):
    def __call__(self, cmp: Any = ..., lt: Any = ..., eq: Any = ..., gt: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape006(Protocol):
    def __call__(self, condition: Any = ..., error_msg: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape007(Protocol):
    def __call__(self, condition: Any = ..., tick_once_only: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape008(Protocol):
    def __call__(self, condition: Any = ..., true_value: Any = ..., false_value: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape009(Protocol):
    def __call__(self, condition: Any = ..., ts: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape010(Protocol):
    def __call__(self, condition: Any = ..., ts: Any = ..., buffer_length: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape011(Protocol):
    def __call__(self, condition: Any = ..., ts: Any = ..., delay: Any = ..., buffer_length: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape012(Protocol):
    def __call__(self, data_frame: Any = ..., as_of_time: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape013(Protocol):
    def __call__(self, df: Any = ..., dt_col: Any = ..., key_col: Any = ..., value_col: Any = ..., offset: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape014(Protocol):
    def __call__(self, fmt: Any = ..., __std_out__: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape015(Protocol):
    def __call__(self, fmt: Any = ..., level: Any = ..., sample_count: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape016(Protocol):
    def __call__(self, fn: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape017(Protocol):
    def __call__(self, frames: Any = ..., dt_col: Any = ..., key_col: Any = ..., value_col: Any = ..., offset: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape018(Protocol):
    def __call__(self, func: Any = ..., __key_arg__: Any = ..., __name__: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape019(Protocol):
    def __call__(self, func: Any = ..., __key_arg__: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape020(Protocol):
    def __call__(self, func: Any = ..., __trace_back_depth__: Any = ..., __capture_values__: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape021(Protocol):
    def __call__(self, func: Any = ..., ts: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape022(Protocol):
    def __call__(self, hash: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape023(Protocol):
    def __call__(self, index: Any = ..., ts: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape024(Protocol):
    def __call__(self, instant: Any = ..., zone: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape025(Protocol):
    def __call__(self, key: Any = ..., cases: Any = ..., ts: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape026(Protocol):
    def __call__(self, key: Any = ..., recordable_id: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape027(Protocol):
    def __call__(self, key: Any = ..., recordable_id: Any = ..., tm: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape028(Protocol):
    def __call__(self, key: Any = ..., value: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape029(Protocol):
    def __call__(self, keys: Any = ..., values: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape030(Protocol):
    def __call__(self, label: Any = ..., ts: Any = ..., sample: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape031(Protocol):
    def __call__(self, lhs: Any = ..., rhs: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape032(Protocol):
    def __call__(self, lhs: Any = ..., rhs: Any = ..., recordable_id: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape033(Protocol):
    def __call__(self, local: Any = ..., zone: Any = ..., ambiguous: Any = ..., nonexistent: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape034(Protocol):
    def __call__(self, op: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape035(Protocol):
    def __call__(self, pattern: Any = ..., repl: Any = ..., s: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape036(Protocol):
    def __call__(self, pattern: Any = ..., s: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape037(Protocol):
    def __call__(self, range: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape038(Protocol):
    def __call__(self, range: Any = ..., delta: Any = ..., month_end_policy: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape039(Protocol):
    def __call__(self, range: Any = ..., value: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape040(Protocol):
    def __call__(self, s: Any = ..., separator: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape041(Protocol):
    def __call__(self, s: Any = ..., start: Any = ..., end: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape042(Protocol):
    def __call__(self, signal: Any = ..., ts: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape043(Protocol):
    def __call__(self, start_time: Any = ..., end_time: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape044(Protocol):
    def __call__(self, ts: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape045(Protocol):
    def __call__(self, ts: Any = ..., *args: Any, **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape046(Protocol):
    def __call__(self, ts: Any = ..., alpha: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape047(Protocol):
    def __call__(self, ts: Any = ..., attr: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape048(Protocol):
    def __call__(self, ts: Any = ..., attr: Any = ..., value: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape049(Protocol):
    def __call__(self, ts: Any = ..., by: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape050(Protocol):
    def __call__(self, ts: Any = ..., by: Any = ..., descending: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape051(Protocol):
    def __call__(self, ts: Any = ..., columns: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape052(Protocol):
    def __call__(self, ts: Any = ..., default_value: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape053(Protocol):
    def __call__(self, ts: Any = ..., delta: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape054(Protocol):
    def __call__(self, ts: Any = ..., dt_col: Any = ..., key_col: Any = ..., value_col: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape055(Protocol):
    def __call__(self, ts: Any = ..., idx: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape056(Protocol):
    def __call__(self, ts: Any = ..., item: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape057(Protocol):
    def __call__(self, ts: Any = ..., key: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape058(Protocol):
    def __call__(self, ts: Any = ..., matches: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape059(Protocol):
    def __call__(self, ts: Any = ..., min: Any = ..., max: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape060(Protocol):
    def __call__(self, ts: Any = ..., mode: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape061(Protocol):
    def __call__(self, ts: Any = ..., msg: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape062(Protocol):
    def __call__(self, ts: Any = ..., n_digits: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape063(Protocol):
    def __call__(self, ts: Any = ..., new_keys: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape064(Protocol):
    def __call__(self, ts: Any = ..., partitions: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape065(Protocol):
    def __call__(self, ts: Any = ..., period: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape066(Protocol):
    def __call__(self, ts: Any = ..., period: Any = ..., delay_first_tick: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape067(Protocol):
    def __call__(self, ts: Any = ..., period: Any = ..., min_window_period: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape068(Protocol):
    def __call__(self, ts: Any = ..., predicate: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape069(Protocol):
    def __call__(self, ts: Any = ..., start: Any = ..., stop: Any = ..., step_size: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape070(Protocol):
    def __call__(self, ts: Any = ..., step_size: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape071(Protocol):
    def __call__(self, ts1: Any = ..., ts2: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape072(Protocol):
    def __call__(self, tsb: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape073(Protocol):
    def __call__(self, tsd: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape074(Protocol):
    def __call__(self, tsl: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape075(Protocol):
    def __call__(self, tsw: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape076(Protocol):
    def __call__(self, value: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape077(Protocol):
    def __call__(self, value: Any = ..., quantum: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape078(Protocol):
    def __call__(self, value: Any = ..., width: Any = ..., origin: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape079(Protocol):
    def __call__(self, value: Any = ..., zone: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape080(Protocol):
    def __call__(self, window: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

class _OperatorShape081(Protocol):
    def __call__(self, x: Any = ..., **kwargs: Any) -> WiringPort | None: ...
    def __getitem__(self, item: Any, /) -> Self: ...

abs_: _OperatorShape044
add_: _OperatorShape031
all_: _OperatorShape000
and_: _OperatorShape031
any_: _OperatorShape000
apply: _OperatorShape016
as_array: _OperatorShape075
assert_: _OperatorShape006
at_zone: _OperatorShape024
batch: _OperatorShape011
bit_and: _OperatorShape000
bit_or: _OperatorShape000
bit_xor: _OperatorShape000
call: _OperatorShape016
clip: _OperatorShape059
cmp_: _OperatorShape031
collapse_keys: _OperatorShape044
collect: _OperatorShape000
combine: _OperatorShape000
combine_cs: _OperatorShape044
combine_json: _OperatorShape000
combine_map: _OperatorShape029
combine_tsd: _OperatorShape000
combine_tss_from_tsl: _OperatorShape044
compare: _OperatorShape032
concat: _OperatorShape071
const: _OperatorShape076
contains_: _OperatorShape056
convert: _OperatorShape000
convert_zone: _OperatorShape079
corrcoef: _OperatorShape081
count: _OperatorShape044
cumsum: _OperatorShape002
day: _OperatorShape044
day_of_month: _OperatorShape044
days: _OperatorShape044
debug_print: _OperatorShape030
dedup: _OperatorShape044
default: _OperatorShape052
dereference: _OperatorShape072
diff: _OperatorShape044
difference: _OperatorShape045
dispatch_: _OperatorShape004
div_: _OperatorShape031
divmod_: _OperatorShape031
downcast_: _OperatorShape044
downcast_ref: _OperatorShape044
drop: _OperatorShape044
emit: _OperatorShape044
eq_: _OperatorShape031
evaluation_time_in_range: _OperatorShape043
ewma: _OperatorShape046
explode: _OperatorShape044
filter_: _OperatorShape009
filter_cs: _OperatorShape068
filter_frame: _OperatorShape068
filter_tsd_by_matches: _OperatorShape058
flip: _OperatorShape044
flip_keys: _OperatorShape044
floordiv_: _OperatorShape031
format_: _OperatorShape001
freeze: _OperatorShape000
from_data_frame: _OperatorShape013
from_data_frame_batches: _OperatorShape017
from_json: _OperatorShape044
from_table: _OperatorShape044
from_table_const: _OperatorShape076
gate: _OperatorShape010
ge_: _OperatorShape031
get_item: _OperatorShape055
getattr_: _OperatorShape047
getitem_: _OperatorShape044
group_by: _OperatorShape049
gt_: _OperatorShape031
hour: _OperatorShape044
if_: _OperatorShape009
if_cmp: _OperatorShape005
if_then_else: _OperatorShape008
if_true: _OperatorShape007
index_of: _OperatorShape056
intersection: _OperatorShape045
invert_: _OperatorShape044
is_empty: _OperatorShape044
isoformat: _OperatorShape044
isoweekday: _OperatorShape044
join: _OperatorShape000
json_as_bool: _OperatorShape044
json_as_float: _OperatorShape044
json_as_int: _OperatorShape044
json_as_str: _OperatorShape044
json_decode: _OperatorShape044
json_encode: _OperatorShape044
keys_: _OperatorShape044
lag: _OperatorShape044
last_modified_date: _OperatorShape044
last_modified_time: _OperatorShape044
last_modified_wall_clock_time: _OperatorShape044
le_: _OperatorShape031
len_: _OperatorShape044
ln: _OperatorShape044
log_: _OperatorShape015
lshift_: _OperatorShape031
lt_: _OperatorShape031
make_tsd: _OperatorShape028
map_: _OperatorShape019
match_: _OperatorShape036
max_: _OperatorShape000
max_ts_list: _OperatorShape074
mean: _OperatorShape000
merge: _OperatorShape000
merge_tsd_disjoint: _OperatorShape074
mesh_: _OperatorShape018
microsecond: _OperatorShape044
microseconds: _OperatorShape044
min_: _OperatorShape000
min_ts_list: _OperatorShape074
minute: _OperatorShape044
mod_: _OperatorShape031
modified: _OperatorShape044
month: _OperatorShape044
month_of_year: _OperatorShape044
mul_: _OperatorShape031
ne_: _OperatorShape031
neg_: _OperatorShape044
not_: _OperatorShape044
nothing: _OperatorShape000
np_std: _OperatorShape044
null_sink: _OperatorShape044
or_: _OperatorShape031
partition: _OperatorShape064
pct_change: _OperatorShape044
pos_: _OperatorShape044
pow_: _OperatorShape031
print_: _OperatorShape014
quantile: _OperatorShape003
race: _OperatorShape045
range_adjacent: _OperatorShape031
range_contains: _OperatorShape039
range_difference: _OperatorShape031
range_extent: _OperatorShape037
range_hull: _OperatorShape031
range_intersection: _OperatorShape031
range_merge: _OperatorShape031
range_mergeable: _OperatorShape031
range_overlaps: _OperatorShape031
range_shift: _OperatorShape038
range_touches: _OperatorShape031
range_union: _OperatorShape031
record: _OperatorShape057
reduce: _OperatorShape021
reduce_tsd_of_bundles_with_race: _OperatorShape073
reduce_tsd_with_race: _OperatorShape073
rekey: _OperatorShape063
replace: _OperatorShape035
replay: _OperatorShape026
replay_const: _OperatorShape027
replay_data_frame: _OperatorShape012
request_id: _OperatorShape022
resample: _OperatorShape065
resolve_civil: _OperatorShape033
rolling_average: _OperatorShape067
rolling_window_arrays: _OperatorShape080
round_: _OperatorShape062
route_by_index: _OperatorShape023
rshift_: _OperatorShape031
sample: _OperatorShape042
schedule: _OperatorShape000
second: _OperatorShape044
seconds: _OperatorShape044
setattr_: _OperatorShape048
sign: _OperatorShape044
slice_: _OperatorShape069
sorted_: _OperatorShape050
split: _OperatorShape040
std: _OperatorShape000
step: _OperatorShape070
stop_engine: _OperatorShape061
str_: _OperatorShape044
sub_: _OperatorShape000
substr: _OperatorShape041
sum_: _OperatorShape000
switch_: _OperatorShape025
symmetric_difference: _OperatorShape045
take: _OperatorShape044
temporal_bucket: _OperatorShape078
temporal_ceil: _OperatorShape077
temporal_floor: _OperatorShape077
temporal_round: _OperatorShape077
throttle: _OperatorShape066
timestamp: _OperatorShape044
to_civil: _OperatorShape076
to_data_frame: _OperatorShape054
to_instant: _OperatorShape076
to_json: _OperatorShape053
to_table: _OperatorShape060
to_window: _OperatorShape067
total_seconds: _OperatorShape044
try_except: _OperatorShape020
type_: _OperatorShape044
uncollapse_keys: _OperatorShape044
ungroup: _OperatorShape044
union: _OperatorShape045
unpartition: _OperatorShape044
until_true: _OperatorShape000
valid: _OperatorShape044
values_: _OperatorShape044
var: _OperatorShape000
weekday: _OperatorShape044
window: _OperatorShape067
with_columns: _OperatorShape051
year: _OperatorShape044
zero: _OperatorShape034

__all__ = (
    "abs_",
    "add_",
    "all_",
    "and_",
    "any_",
    "apply",
    "as_array",
    "assert_",
    "at_zone",
    "batch",
    "bit_and",
    "bit_or",
    "bit_xor",
    "call",
    "clip",
    "cmp_",
    "collapse_keys",
    "collect",
    "combine",
    "combine_cs",
    "combine_json",
    "combine_map",
    "combine_tsd",
    "combine_tss_from_tsl",
    "compare",
    "concat",
    "const",
    "contains_",
    "convert",
    "convert_zone",
    "corrcoef",
    "count",
    "cumsum",
    "day",
    "day_of_month",
    "days",
    "debug_print",
    "dedup",
    "default",
    "dereference",
    "diff",
    "difference",
    "dispatch_",
    "div_",
    "divmod_",
    "downcast_",
    "downcast_ref",
    "drop",
    "emit",
    "eq_",
    "evaluation_time_in_range",
    "ewma",
    "explode",
    "filter_",
    "filter_cs",
    "filter_frame",
    "filter_tsd_by_matches",
    "flip",
    "flip_keys",
    "floordiv_",
    "format_",
    "freeze",
    "from_data_frame",
    "from_data_frame_batches",
    "from_json",
    "from_table",
    "from_table_const",
    "gate",
    "ge_",
    "get_item",
    "getattr_",
    "getitem_",
    "group_by",
    "gt_",
    "hour",
    "if_",
    "if_cmp",
    "if_then_else",
    "if_true",
    "index_of",
    "intersection",
    "invert_",
    "is_empty",
    "isoformat",
    "isoweekday",
    "join",
    "json_as_bool",
    "json_as_float",
    "json_as_int",
    "json_as_str",
    "json_decode",
    "json_encode",
    "keys_",
    "lag",
    "last_modified_date",
    "last_modified_time",
    "last_modified_wall_clock_time",
    "le_",
    "len_",
    "ln",
    "log_",
    "lshift_",
    "lt_",
    "make_tsd",
    "map_",
    "match_",
    "max_",
    "max_ts_list",
    "mean",
    "merge",
    "merge_tsd_disjoint",
    "mesh_",
    "microsecond",
    "microseconds",
    "min_",
    "min_ts_list",
    "minute",
    "mod_",
    "modified",
    "month",
    "month_of_year",
    "mul_",
    "ne_",
    "neg_",
    "not_",
    "nothing",
    "np_std",
    "null_sink",
    "or_",
    "partition",
    "pct_change",
    "pos_",
    "pow_",
    "print_",
    "quantile",
    "race",
    "range_adjacent",
    "range_contains",
    "range_difference",
    "range_extent",
    "range_hull",
    "range_intersection",
    "range_merge",
    "range_mergeable",
    "range_overlaps",
    "range_shift",
    "range_touches",
    "range_union",
    "record",
    "reduce",
    "reduce_tsd_of_bundles_with_race",
    "reduce_tsd_with_race",
    "rekey",
    "replace",
    "replay",
    "replay_const",
    "replay_data_frame",
    "request_id",
    "resample",
    "resolve_civil",
    "rolling_average",
    "rolling_window_arrays",
    "round_",
    "route_by_index",
    "rshift_",
    "sample",
    "schedule",
    "second",
    "seconds",
    "setattr_",
    "sign",
    "slice_",
    "sorted_",
    "split",
    "std",
    "step",
    "stop_engine",
    "str_",
    "sub_",
    "substr",
    "sum_",
    "switch_",
    "symmetric_difference",
    "take",
    "temporal_bucket",
    "temporal_ceil",
    "temporal_floor",
    "temporal_round",
    "throttle",
    "timestamp",
    "to_civil",
    "to_data_frame",
    "to_instant",
    "to_json",
    "to_table",
    "to_window",
    "total_seconds",
    "try_except",
    "type_",
    "uncollapse_keys",
    "ungroup",
    "union",
    "unpartition",
    "until_true",
    "valid",
    "values_",
    "var",
    "weekday",
    "window",
    "with_columns",
    "year",
    "zero",
)
