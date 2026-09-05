/**
 * Optional Python bridge for the C++ runtime.
 *
 * This module exposes wiring, graph execution, services/adaptors, Python
 * user-authored nodes, value conversion, and the record/replay test harness
 * through nanobind. The runtime, schemas, dispatch, and value storage remain
 * C++-owned; this file is the compatibility/binding surface.
 */
#include "module_internal.h"
#include <hgraph/python/retained_value.h>
#include "py_bindings.h"
#include "py_wiring.h"

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
#include <hgraph/lib/std/operators/table_rows.h>   // ts_table_layout (table_schema_info)
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
#include <hgraph/python/native_scalar_registration.h>

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

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace
{
    std::uint64_t python_registry_generation{0};

    /** The python RequirementsNotMetWiringError class (installed at import). */
    [[nodiscard]] nb::object &requirements_error_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }
}  // namespace

NB_MODULE(_hgraph, m)
{
    nb::register_exception_translator([](const std::exception_ptr &p, void *) {
        try
        {
            std::rethrow_exception(p);
        }
        catch (const OperatorRequirementsError &error)
        {
            nb::object &cls = requirements_error_slot();
            if (cls.is_valid()) { PyErr_SetObject(cls.ptr(), nb::str(error.what()).ptr()); }
            else { PyErr_SetString(PyExc_RuntimeError, error.what()); }
        }
    });
    nb::register_exception_translator([](const std::exception_ptr &p, void *) {
        try
        {
            std::rethrow_exception(p);
        }
        catch (const RetainedGraphRunError &error)
        {
            translate_retained_graph_run_error(error);
        }
    });
    m.def("_set_requirements_error", [](nb::object cls) { requirements_error_slot() = std::move(cls); });
    // The polars-frames compatibility switch (issue #80): outbound Frame /
    // Series surface as polars objects when set. Driven by the hgraph
    // feature-switch mechanism at package import; runtime-togglable for
    // tests. The C++ value substrate stays Arrow either way.
    m.def("set_polars_frames", [](bool enabled) { polars_frames_enabled() = enabled; });
    m.def("polars_frames", [] { return polars_frames_enabled(); });
    m.doc() = "hgraph C++ runtime bridge (slice 1: wire and run graphs by operator name)";

    // The graph-callable records are immortal by design (WiredFn contexts
    // must outlive every value referencing them), so their held Python
    // objects survive interpreter teardown - not a refcount bug.
    nb::set_leak_warnings(false);

    stdlib::register_standard_operators();
    python_bridge::py_infer_value_slot() =
        reinterpret_cast<python_bridge::PyInferValueFn>(&python_bridge::py_to_value);
    // The retained-value storage policy reaches the plan factory only through
    // this table (python_bridge.rst, "Consumer-selected Python value storage").
    python_bridge::register_python_storage_provider();
    // The enum slots read the meta -> python-Enum-class registry (an
    // immortal map; lazily constructed by its accessor, cleared with the
    // registries).
    enum_to_python_slot() = [](const ValueTypeMetaData *meta, long long value) -> nb::object {
        auto &registry = python_bridge::enum_to_python_registry();
        if (const auto type = registry.find(meta); type != registry.end())
        {
            if (const auto member = type->second.find(value); member != type->second.end())
            {
                return member->second;
            }
        }
        throw std::logic_error(std::string{"enum '"} +
                               (meta->header.label ? meta->header.label : "?") +
                               "' has no registered python member");
    };
    enum_from_python_slot() = [](const ValueTypeMetaData *meta, nb::handle source) -> long long {
        const std::string name = nb::cast<std::string>(source.attr("name"));
        auto &registry = python_bridge::enum_from_python_registry();
        if (const auto type = registry.find(meta); type != registry.end())
        {
            if (const auto member = type->second.find(name); member != type->second.end())
            {
                return member->second;
            }
        }
        throw std::logic_error(std::string{"enum '"} +
                               (meta->header.label ? meta->header.label : "?") +
                               "' has no registered python member '" + name + "'");
    };

    // Route the diagnostic sinks (debug_print / print_) through Python's
    // sys.stdout/sys.stderr. These are native nodes whose schema does not
    // otherwise require the Python phase runner, so the adapter retains its
    // own guard.
    stdlib::io_write_slot() = [](std::string_view line, bool to_stdout) {
        nb::gil_scoped_acquire gil;
        nb::object stream = nb::module_::import_("sys").attr(to_stdout ? "stdout" : "stderr");
        stream.attr("write")(nb::str((std::string{line} + "\n").c_str()));
    };
    register_python_overloads();

    bind_type_system(m);
    bind_ports(m);
    bind_wiring(m);
    bind_state_and_services(m);

    m.def("_registry_generation", [] { return python_registry_generation; });
    // Rebuild the process logger (and its sinks) on demand: spdlog's Windows
    // stdout sinks cache the raw OS handle at construction, so tests that
    // redirect fds per-test (pytest capfd) must reset before logging.
    m.def("reset_logger", [] { hgraph::log::reset_logger(); });
    m.def("reset_registries", [] {
        python_bridge::enum_class_registry().clear();   // meta pointers are re-interned
        python_bridge::enum_type_registry().clear();
        python_bridge::enum_to_python_registry().clear();
        python_bridge::enum_from_python_registry().clear();
        python_bridge::bundle_class_registry().clear();
        python_bridge::bundle_class_info_registry().clear();
        python_bridge::tsb_compound_value_registry().clear();
        python_bridge::clear_python_bundle_bindings();
        python_bridge::clear_native_scalar_types();
        python_bridge::clear_python_opaque_types();
        python_bridge::clear_python_type_registry();   // the reverse binding dies with the metadata
        reset_all_registries();
        ++python_registry_generation;
        stdlib::register_standard_operators();
        register_builtin_native_scalar_types();
        register_python_overloads();
    });
}
