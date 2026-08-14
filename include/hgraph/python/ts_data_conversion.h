#ifndef HGRAPH_PYTHON_TS_DATA_CONVERSION_H
#define HGRAPH_PYTHON_TS_DATA_CONVERSION_H

#include <hgraph/config.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>
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

        /**
         * Python-authoring policy attached to one realized TSData strategy.
         *
         * ``from_python_impl`` on ``TSDataOps`` imports a replacement/current
         * value into live storage.  These operations are deliberately
         * separate: an authored delta and a Python compute-node result have
         * collection-specific semantics (for example TSS set-vs-frozenset and
         * strict TSD removals) that are not replacement assignment.
         *
         * Concrete TSData factories select this passive table once.  Python
         * bridge callers dispatch through it and must not reconstruct a TS
         * shape by switching on ``TSTypeKind``.
         */
        struct PythonTSDataOps
        {
            /** True when this authored value (including nested children) needs
                the wider authored-delta schema. */
            bool (*requires_authored_delta_impl)(TSRoleTypeRef type, nb::handle source);

            /** Build the delta in the schema selected by the preceding query. */
            Value (*delta_from_python_impl)(TSRoleTypeRef type, nb::handle source,
                                            bool authored);
            void (*apply_result_impl)(const TSOutputView &output, nb::handle result);
        };

        /** Canonical throwing table for representations without Python authoring support. */
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &missing_python_ts_data_ops() noexcept;

        /** Strategy tables installed by the corresponding TSData factories. */
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &atomic_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &ref_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &set_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &dict_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &list_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &bundle_python_ts_data_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const PythonTSDataOps &window_python_ts_data_ops() noexcept;

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
