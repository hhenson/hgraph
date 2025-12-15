//
// Created by Claude on 15/12/2025.
//
// Header for TimeSeriesTypeMeta Python bindings
//

#ifndef HGRAPH_TS_TYPE_META_BINDINGS_H
#define HGRAPH_TS_TYPE_META_BINDINGS_H

#include <nanobind/nanobind.h>

namespace hgraph {

/**
 * Register TimeSeriesTypeMeta and related types with nanobind.
 *
 * This exposes:
 * - TimeSeriesKind enum (TS, TSS, TSD, TSL, TSB, TSW)
 * - TimeSeriesTypeMeta class with type_name_str() method
 * - Factory functions: get_ts_type_meta, get_tss_type_meta, get_tsd_type_meta,
 *   get_tsl_type_meta, get_tsb_type_meta, get_tsw_type_meta
 */
void register_ts_type_meta_with_nanobind(nanobind::module_ &m);

} // namespace hgraph

#endif // HGRAPH_TS_TYPE_META_BINDINGS_H
