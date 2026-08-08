/**
 * Cross-TU carrier structs of the Python bridge: the small handle/value
 * types handed to and from Python (type handles, ports, patterns, node and
 * graph-fn records, senders, service descriptors). Promoted out of
 * module.cpp's anonymous namespace: nanobind identifies bound types by
 * std::type_index, so any type crossing a translation-unit boundary MUST
 * have external linkage (an anonymous-namespace duplicate would register as
 * a different type at runtime).
 */
#ifndef HGRAPH_PYTHON_PY_CARRIERS_H
#define HGRAPH_PYTHON_PY_CARRIERS_H

#include "module_internal.h"

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/time_series/ts_output/alternative.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/lib/std/component.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/json.h>
#include <hgraph/lib/std/operators/impl/io_impl.h>   // io_write_slot (sys.stdout routing)
#include <hgraph/lib/std/operators/impl/table_impl.h>   // ts_table_layout (table_schema_info)
#include <hgraph/runtime/logger.h>       // log::reset_logger (test support)
#include <hgraph/runtime/node_error.h>   // node_error_ts_meta (exception_time_series)
#include <hgraph/types/value/json_codec.h>          // to_json_string / from_json_string (builders)
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/service_runtime.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/convert_target.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/util/scope.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/python/chrono.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace hgraph::python_bridge
{
    // ---------------------------------------------------------------
    // Wiring surface
    // ---------------------------------------------------------------

    struct PyTsType
    {
        const TSValueTypeMetaData *meta{nullptr};
    };

    /** Internal scalar used to carry a resolved TS schema into a generic
        Python node implementation (for its hidden recordable-state output). */
    struct PyTsMetaRef
    {
        const TSValueTypeMetaData *meta{nullptr};
        friend bool operator==(const PyTsMetaRef &, const PyTsMetaRef &) noexcept = default;
    };

    struct PyValueType
    {
        const ValueTypeMetaData *meta{nullptr};
    };

    struct PyScalarPattern
    {
        ScalarPattern pattern{};
    };

    struct PySizePattern
    {
        bool        variable{false};
        std::string name{};
        std::size_t value{0};
    };

    struct PyTypePattern
    {
        TypePattern pattern{};
    };

    struct PyWiredFn
    {
        WiredFn fn{};
    };

    struct PyNodeRecord;

    /** The user-node callable scalar (immortal record; identity by pointer). */
    struct PyNodeRef
    {
        const PyNodeRecord *record{nullptr};
        friend bool operator==(const PyNodeRef &, const PyNodeRef &) noexcept = default;
    };

    struct PyNodeHandle
    {
        const PyNodeRecord *record{nullptr};
    };

    /**
     * The python push-source sender: the slot is filled by the node's
     * on_start (graph thread); python threads convert and send through it.
     * shared_ptr is sanctioned here - this IS the cross-thread boundary.
     */
    struct PySenderSlot
    {
        PushSourceSender           sender{};
        const TSValueTypeMetaData *schema{nullptr};
        const TypeRealizationSnapshot *type_realization{nullptr};
    };

    struct PySender
    {
        std::shared_ptr<PySenderSlot> slot;

        void send(nb::handle object) const
        {
            if (slot == nullptr || !slot->sender.valid())
            {
                throw std::logic_error("push sender is not started yet (the graph must be running)");
            }
            TypeRealizationScope realization_scope{slot->type_realization};
            Value value = py_to_delta(object, slot->schema);
            nb::gil_scoped_release release;
            slot->sender.send(std::move(value));
        }
    };

    /** An opaque pre-converted scalar Value (e.g. the user-node scalars list). */
    struct PyServiceDesc
    {
        const RuntimeServiceDescriptor *descriptor{nullptr};
    };

    struct PyScalarValue
    {
        Value value{};
    };

    // ---------------------------------------------------------------
    // Python graph callables as WiredFn values (Howard's ruling: the
    // type-erased context+ops pattern, so Python and C++ backends coexist;
    // identity = the user function object).
    // ---------------------------------------------------------------

    /** The python DSL's wiring-time resolution window onto the C++
        type/pattern machinery (bound as ``ResolutionScope``). */
    struct PyResolutionScope
    {
        ResolutionMap map{};
    };

    struct PyGraphFnRecord
    {
        nb::object                    wrapper;   ///< package-side: wrapper(borrowed_wiring, ports) -> port|None
        nb::object                    user_fn;   ///< semantic callable exposed to requires/resolvers
        nb::object                    identity;  ///< registry identity anchor + keepalive
        std::string                   diagnostic_label;
        std::vector<std::string>      name_storage;
        std::vector<std::string_view> names;
        std::vector<const TSValueTypeMetaData *> input_schemas;
        std::vector<std::optional<TypePattern>> input_patterns;
        std::size_t                   arity{0};
        bool                          has_output{true};
        /** The annotated output schema, when known (mesh_ needs the element
            type ahead of compilation). Null = resolve at compile. */
        const TSValueTypeMetaData    *output_schema{nullptr};
    };

    struct PyPort
    {
        WiringPortRef ref{};
    };

    struct PyRun;

    struct PySwitchCases
    {
        stdlib::SwitchCases cases{};
    };

    struct PyDispatchCases
    {
        stdlib::DispatchCases cases{};
    };

    /** hgraph's feedback: an unbound source port bound later to close a cycle. */
    struct PyFeedback
    {
        Wiring                    *wiring{nullptr};
        WiringPortRef              delegate{};
        const TSValueTypeMetaData *schema{nullptr};
        bool                       bound{false};
    };

    /** Python carrier for the C++ wiring-core delayed binding handle. */
    struct PyDelayedBinding
    {
        ErasedDelayedBindingWiringPort binding{};
    };

    struct PyNodeRecord
    {
        nb::object fn;
    };
}  // namespace hgraph::python_bridge

template <>
struct std::hash<hgraph::python_bridge::PyNodeRef>
{
    [[nodiscard]] std::size_t operator()(const hgraph::python_bridge::PyNodeRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.record);
    }
};

template <>
struct std::hash<hgraph::python_bridge::PyTsMetaRef>
{
    [[nodiscard]] std::size_t operator()(const hgraph::python_bridge::PyTsMetaRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.meta);
    }
};

namespace hgraph::static_schema_detail
{
    using python_bridge::PyNodeRef;
    using python_bridge::PyTsMetaRef;

    template <>
    struct scalar_name<PyNodeRef>
    {
        static constexpr std::string_view value{"py_node_ref"};
    };


    template <>
    struct scalar_name<PyTsMetaRef>
    {
        static constexpr std::string_view value{"PyTsMetaRef"};
    };

}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_PYTHON_PY_CARRIERS_H
