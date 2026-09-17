#include "py_bindings.h"
#include "py_wiring.h"

#include <hgraph/runtime/spawn.h>
#include <nanobind/stl/string.h>

namespace hgraph::python_bridge
{
    namespace
    {
        void run_spawn_phase(GraphExecutorPhase, GraphExecutorPhaseAction action)
        {
            nb::gil_scoped_acquire acquire;
            try { action(); }
            catch (const nb::python_error &error)
            {
                // Python exception objects must be formatted and destroyed
                // while the GIL is held, before crossing the worker boundary.
                throw std::runtime_error(error.what());
            }
        }

        // Parent phases hold the GIL; blocking while a child needs it would
        // deadlock. Native executor idle waits can enter without holding it.
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
                           std::size_t capacity_bytes) {
            if (wiring.finished) throw nb::value_error("Wiring is already finished");
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
                pipeline.stages.push_back(std::move(stage));
            }
            auto arguments = build_args(args, kwargs);
            wire_spawn(wiring.wiring_ref(), std::move(pipeline), arguments,
                       SpawnConfig{capacity_frames, capacity_bytes},
                       &run_spawn_phase, &run_spawn_wait);
        }, nb::arg("wiring"), nb::arg("stages"), nb::arg("args"),
           nb::arg("kwargs"), nb::arg("capacity_frames"), nb::arg("capacity_bytes"));
    }
}
