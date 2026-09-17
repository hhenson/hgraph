#include "py_bindings.h"
#include "py_wiring.h"

#include <hgraph/runtime/spawn.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <cmath>

namespace hgraph::python_bridge
{
    namespace
    {
        // Release the owner's GIL while waiting on process transport.
        void run_spawn_wait(const std::function<void()> &action)
        {
            // Ensure/release nesting preserves the caller's original GIL
            // state and uses only the stable ABI (PyGILState_Check does not).
            nb::gil_scoped_acquire acquire;
            nb::gil_scoped_release release;
            action();
        }
    }

    void bind_spawn(nb::module_ &m)
    {
        m.def("spawn", [](PyWiring &wiring, nb::list stages, nb::tuple args,
                           nb::dict kwargs, std::size_t capacity_frames,
                           std::size_t capacity_bytes, double timeout_seconds,
                           const std::string &program, const std::vector<std::string> &worker_arguments) {
            if (wiring.finished) throw nb::value_error("Wiring is already finished");
            if (!std::isfinite(timeout_seconds) || timeout_seconds <= 0 || timeout_seconds > 86'400)
                throw nb::value_error("spawn_: worker timeout must be positive and at most 24 hours");
            SpawnPipeline pipeline;
            pipeline.stages.reserve(nb::len(stages));
            for (auto item : stages)
            {
                auto description = nb::cast<nb::tuple>(item);
                SpawnStage stage;
                stage.function = nb::cast<PyWiredFn &>(description[0]).fn;
                for (auto binding : nb::cast<nb::dict>(description[1]))
                    stage.bindings.emplace_back(nb::cast<std::string>(binding.first),
                                                nb::cast<PyPort &>(binding.second).ref);
                stage.recipe = "python";
                const auto bootstrap = nb::cast<std::string>(description[2]);
                stage.describe = [bootstrap](std::span<const TSValueTypeMetaData *const> inputs) {
                    auto json = nb::module_::import_("json");
                    auto recipe = nb::cast<nb::dict>(json.attr("loads")(bootstrap));
                    nb::list schemas;
                    for (auto schema : inputs)
                        schemas.append(nb::module_::import_("_hgraph").attr("_distributed_describe_ts")(PyTsType{schema}));
                    recipe["inputs"] = schemas;
                    return nb::cast<std::string>(json.attr("dumps")(recipe));
                };
                pipeline.stages.push_back(std::move(stage));
            }
            auto arguments = build_args(args, kwargs);
            wire_spawn(wiring.wiring_ref(), std::move(pipeline), arguments,
                       SpawnConfig{capacity_frames, capacity_bytes,
                           std::chrono::milliseconds{static_cast<std::int64_t>(std::ceil(timeout_seconds * 1000))},
                           program, worker_arguments}, &run_spawn_wait);
        }, nb::arg("wiring"), nb::arg("stages"), nb::arg("args"),
           nb::arg("kwargs"), nb::arg("capacity_frames"), nb::arg("capacity_bytes"),
           nb::arg("timeout_seconds"), nb::arg("program"), nb::arg("worker_arguments"));
    }
}
