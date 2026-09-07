# Standard-library migration inventory

Status: baseline inventory from hgraph `baa51d58e`; migration not started

This inventory makes the current C++ library finite before any implementation
is removed. The public headers contain 212 operator marker declarations with
207 distinct native registry names. Repeated names include multiple marker
shapes and the cross-family `join` overload family. Registration helpers and
templates expand this into a substantially larger candidate set, so marker
count is not candidate count.

## Public operator contracts

| Native family | Distinct names | Registry names |
| --- | ---: | --- |
| arithmetic | 14 | `abs_`, `add_`, `div_`, `divmod_`, `floordiv_`, `ln`, `mod_`, `mul_`, `neg_`, `pos_`, `pow_`, `round_`, `sign`, `sub_` |
| collection | 22 | `collapse_keys`, `combine_cs`, `combine_map`, `combine_tsd`, `difference`, `filter_tsd_by_matches`, `flip`, `flip_keys`, `intersection`, `keys_`, `make_tsd`, `max_ts_list`, `mean`, `min_ts_list`, `partition`, `rekey`, `sum_`, `symmetric_difference`, `uncollapse_keys`, `union`, `unpartition`, `values_` |
| comparison | 9 | `cmp_`, `eq_`, `ge_`, `gt_`, `le_`, `lt_`, `max_`, `min_`, `ne_` |
| container | 8 | `contains_`, `dereference`, `getattr_`, `getitem_`, `index_of`, `is_empty`, `len_`, `setattr_` |
| control | 12 | `all_`, `any_`, `if_`, `if_cmp`, `if_then_else`, `if_true`, `merge`, `merge_tsd_disjoint`, `race`, `reduce_tsd_of_bundles_with_race`, `reduce_tsd_with_race`, `route_by_index` |
| conversion | 14 | `cast_`, `collect`, `combine`, `combine_tss_from_tsl`, `const`, `convert`, `default`, `downcast_`, `downcast_ref`, `emit`, `nothing`, `str_`, `type_`, `zero` |
| data frame | 12 | `concat`, `filter_cs`, `filter_frame`, `from_data_frame`, `from_data_frame_batches`, `group_by`, `join`, `replay_data_frame`, `sorted_`, `to_data_frame`, `ungroup`, `with_columns` |
| higher order | 6 | `dispatch_`, `map_`, `mesh_`, `reduce`, `switch_`, `try_except` |
| I/O and record/replay | 17 | `apply`, `__apply_value_callable`, `assert_`, `__assert_fmt`, `call`, `__call_value_callable`, `compare`, `debug_print`, `log_`, `__log_sink`, `null_sink`, `print_`, `__print_sink`, `record`, `replay`, `replay_const`, `stop_engine` |
| JSON | 11 | `combine_json`, `from_json`, `__json_array`, `json_as_bool`, `json_as_float`, `json_as_int`, `json_as_str`, `json_decode`, `json_encode`, `__json_object`, `to_json` |
| logical and bitwise | 9 | `and_`, `bit_and`, `bit_or`, `bit_xor`, `invert_`, `lshift_`, `not_`, `or_`, `rshift_` |
| stream | 19 | `batch`, `dedup`, `drop`, `filter_`, `filter_by`, `freeze`, `gate`, `lag`, `__lag_proxy`, `request_id`, `sample`, `schedule`, `slice_`, `step`, `take`, `throttle`, `to_window`, `until_true`, `window` |
| string | 6 | `format_`, `join`, `match_`, `replace`, `split`, `substr` |
| table | 3 | `from_table`, `from_table_const`, `to_table` |
| temporal | 46 | `at_zone`, `convert_zone`, `datepart`, `day`, `day_of_month`, `days`, `evaluation_time_in_range`, `explode`, `hour`, `isoformat`, `isoweekday`, `last_modified_date`, `last_modified_time`, `last_modified_wall_clock_time`, `microsecond`, `microseconds`, `minute`, `modified`, `month`, `month_of_year`, `range_adjacent`, `range_contains`, `range_difference`, `range_extent`, `range_hull`, `range_intersection`, `range_merge`, `range_mergeable`, `range_overlaps`, `range_shift`, `range_touches`, `range_union`, `resolve_civil`, `second`, `seconds`, `temporal_bucket`, `temporal_ceil`, `temporal_floor`, `temporal_round`, `timestamp`, `to_civil`, `to_instant`, `total_seconds`, `valid`, `weekday`, `year` |

The authoritative contracts remain the headers under
`include/hgraph/lib/std/operators`. This table is a migration checkpoint and
must be refreshed when that surface changes.

The HGL prototype declares 197 distinct source-visible names. The ten public-
header registry names not declared as HGL operators are the keyword collisions
`const` and `default`, plus the compiler-selected internal markers
`__apply_value_callable`, `__assert_fmt`, `__call_value_callable`,
`__json_array`, `__json_object`, `__lag_proxy`, `__log_sink`, and
`__print_sink`. The implementation-only `__tick_count` marker is not declared
in a public operator header and is outside this contract inventory.

## Other library surfaces

| Surface | Current implementation | Initial classification |
| --- | --- | --- |
| Standard scalar/type registration | `standard_types.h`, `standard_scalar_bindings.cpp` | Runtime/type-spec generated contract plus backend implementation. |
| Scalar kernels and lifting | `lifted_kernels.h` | Split: scalar algorithms may remain reviewed native functions; temporal lifting should be generated from HGL candidates. |
| `pass_through_node` | `std_nodes.h` | HGL runtime candidate, blocked on a first-class delta capture/apply contract. |
| `component` | `component.h` | HGL graph/library feature, blocked on record/replay scopes, variadic named inputs, and graph metadata. |
| `lower` | `lower.h`, `lower.cpp` | Host execution API, not an HGL graph/node implementation. |
| Higher-order map/reduce/switch/mesh | public operators plus runtime nodes | Compiler/runtime kernel. User-facing specializations may be HGL; child ownership and scheduling remain native contracts. |
| Data-frame, table, JSON, I/O | operator families and native implementations | Mostly constrained native primitives with HGL graph wrappers where composition adds behavior. |

## Migration states

- **contract** — public operator declaration represented in HGL.
- **source candidate** — an HGL `impl fn` describes the current behavior.
- **blocked** — the exact missing language or public runtime contract is named.
- **kernel** — reviewed reason the implementation remains native.
- **migrated** — HGL source builds the shipped candidate, parity passes, and the
  former hand-written implementation is removed or delegates to it.

No item in this inventory is yet `migrated`.

## First extraction slice

The source prototype deliberately starts with:

1. scalar arithmetic/comparison candidates, testing runtime scalar operators
   inside an implementation of the corresponding temporal operator;
2. `sample`, `filter_`, `dedup`, `take`, and `drop`, testing activation,
   recordable state, prior output, and generic equality;
3. fixed-list `len_`, `sum_`, and `mean`, testing constant generics and
   evaluation-time traversal;
4. `if_then_else` and binary `merge`, testing graph versus node selection;
5. `pass_through_node`, testing portable delta capture/apply;
6. contract-only declarations for the remaining families, exposing variadic,
   keyword-pack, output-resolution, native-view, and resource boundaries.

The blockers discovered by these files are maintained in
[`requirements.md`](requirements.md).

## Definition of migrated

An implementation is migrated only when:

- the HGL source contains no unresolved provisional syntax;
- it compiles through the shared HGraph IR and formatted C++ backend;
- the generated code uses public hgraph contracts and is readable;
- native C++ and Python behavior/parity tests pass through the public operator;
- lifecycle, delta, invalid-input, and performance behavior is preserved;
- the generated module participates in the same provider transaction and
  operator registry; and
- the hand-written C++ implementation is removed or becomes an explicitly
  classified native primitive.
