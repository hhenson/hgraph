#ifndef HGRAPH_PYTHON_TS_DATA_CONVERSION_H
#define HGRAPH_PYTHON_TS_DATA_CONVERSION_H

#include <hgraph/config.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/ts_type_ref.h>

#include <nanobind/nanobind.h>

namespace hgraph
{
    class TSOutputView;
    class Value;
    struct TSValueTypeMetaData;

    namespace python_bridge
    {
        namespace nb = nanobind;

        /** The table is ``hgraph::PythonTSDataOps`` (``ts_data/ops.h``); the
            bridge defines the per-family instances and the canonical throwing
            default is the type layer's ``ts_data_detail::missing_python_ts_data_ops``. */
        /** The authoring table for the family a factory recorded on its ops
            (RFC 0035); ``none`` answers the throwing default. */
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &python_ts_data_ops_for(const TSDataOps &ops) noexcept;

        /** Strategy tables installed by the corresponding TSData factories. */
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &atomic_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &ref_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &set_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &dict_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &list_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &bundle_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &window_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &target_link_python_ts_data_ops() noexcept;

        /** Convert a Python-authored value into an owned canonical/authored delta. */
        [[nodiscard]] HGRAPH_EXPORT Value delta_from_python(
            const TSValueTypeMetaData *schema, nb::handle source);
        [[nodiscard]] HGRAPH_EXPORT Value delta_from_python(
            TSRoleTypeRef type, nb::handle source);

        /** Apply one non-None Python compute-node result through the output's selected strategy. */
        HGRAPH_EXPORT void apply_python_result(const TSOutputView &output, nb::handle result);
    }  // namespace python_bridge
}  // namespace hgraph

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_TS_DATA_CONVERSION_H
