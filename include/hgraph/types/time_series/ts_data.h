// types/time_series/ts_data.h — umbrella for the TS data substrate: the
// payload+delta storage and views that BOTH TSOutput (owning endpoints) and
// TSInput (non-owning proxies) are implemented over. Key invariants live
// here: last_modified_time == evaluation_time means ticked-this-cycle,
// == MIN_DT means never valid; delta cleanup is LAZY (no end-of-cycle sweep —
// deltas reset on the next mutation, reads gate on modified()). Runtime
// TSData addresses published through outputs, inputs, proxies, observers, or
// parent links are stable; slot-backed TSData is mutated in place and must not
// be relocated through erased storage copy/move hooks. This invariant is
// specific to TSData, not to unrelated scalar Value storage. Design record:
// docs/source/developer_guide/data_structures/.
#ifndef HGRAPH_CPP_ROOT_TS_DATA_H
#define HGRAPH_CPP_ROOT_TS_DATA_H

#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_data/indexed_view.h>
#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/time_series/ts_data/dict_view.h>
#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series/ts_data/window_view.h>
#include <hgraph/types/time_series/ts_data/storage.h>

#endif  // HGRAPH_CPP_ROOT_TS_DATA_H
