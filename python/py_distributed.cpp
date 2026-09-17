#include "py_bindings.h"
#include "py_wiring.h"

#include <hgraph/runtime/distributed_map_wiring.h>
#include <hgraph/types/value/binary_codec.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace hgraph::python_bridge
{
    void bind_distributed(nb::module_ &m)
    {
        m.def("distributed_map", [](PyWiring &wiring, PyWiredFn func, PyPort input,
                                    Int workers, bool in_process, const std::string &module,
                                    const std::string &qualname, const std::string &program,
                                    const std::vector<std::string> &arguments) {
            if (workers <= 0) { throw nb::value_error("dmap_ needs at least one worker"); }
            if (wiring.finished) { throw nb::value_error("Wiring is already finished"); }
            auto plan = distributed::prepare_distributed_map(func.fn, input.ref.schema);
            // Refuse unsupported boundary values before starting any process.
            for (std::size_t i = 0; i < plan.slots.size(); ++i)
                (void)binary_converter(plan.slots.schema_at(i));
            plan.config.workers = static_cast<std::size_t>(workers);
            plan.config.hosting = in_process ? distributed::WorkerHosting::InProcess
                                            : distributed::WorkerHosting::Process;
            plan.config.program = program;
            plan.config.arguments = arguments;
            plan.phase_runner = &py_run_executor_phase;
            nb::dict recipe;
            recipe["module"] = module;
            recipe["qualname"] = qualname;
            const auto describe = nb::module_::import_("hgraph._distributed").attr("_describe_type");
            const auto annotation = nb::module_::import_("_hgraph").attr("python_type_for_value");
            recipe["key"] = describe(annotation(PyValueType{input.ref.schema->key_type()}));
            recipe["value"] = describe(annotation(PyValueType{input.ref.schema->element_ts()->value_type}));
            recipe["result"] = std::string{plan.output->element_ts()->value_type->name()};
            plan.recipe = nb::cast<std::string>(nb::module_::import_("json").attr("dumps")(recipe));
            return PyPort{distributed::wire_distributed_map(wiring.wiring_ref(), input.ref,
                std::make_shared<const distributed::DistributedMapPlan>(std::move(plan))).erased()};
        }, nb::arg("wiring"), nb::arg("func"), nb::arg("input"), nb::arg("workers"),
           nb::arg("in_process"), nb::arg("module"), nb::arg("qualname"), nb::arg("program"),
           nb::arg("arguments"));

        m.def("serve_distributed_worker", [](PyWiredFn func, PyValueType key,
                    PyValueType value, const std::string &result,
                    std::int64_t read_handle, std::int64_t write_handle,
                    std::int64_t start, std::int64_t end) {
            auto &registry = TypeRegistry::instance();
            if (!key.meta || !value.meta)
                throw nb::value_error("dmap_ worker input types must be registered by the imported module");
            auto plan = distributed::prepare_distributed_map(func.fn, registry.tsd(key.meta, registry.ts(value.meta)));
            if (plan.output->element_ts()->value_type->name() != result)
                throw nb::value_error("dmap_ worker result schema differs from the caller");
            auto channel = distributed::PipeEndpoint::adopt(read_handle, write_handle);
            // Only complete graph phases acquire the GIL. Blocking transport
            // waits and native execution retain the ordinary runtime policy.
            nb::gil_scoped_release release;
            distributed::serve_worker(channel, std::move(plan.child), plan.slots,
                DateTime{TimeDelta{start}}, DateTime{TimeDelta{end}}, &py_run_executor_phase);
        }, nb::arg("func"), nb::arg("key"), nb::arg("value"), nb::arg("result"),
           nb::arg("read_handle"), nb::arg("write_handle"), nb::arg("start"), nb::arg("end"));
    }
}
