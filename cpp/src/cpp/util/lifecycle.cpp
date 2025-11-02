#include <hgraph/util/lifecycle.h>

namespace hgraph {
    bool ComponentLifeCycle::is_started() const { return _started; }

    bool ComponentLifeCycle::is_starting() const { return _transitioning && !_started; }

    bool ComponentLifeCycle::is_stopping() const { return _transitioning && _started; }

    void ComponentLifeCycle::register_with_nanobind(nb::module_ &m) {
        nb::class_<ComponentLifeCycle, nb::intrusive_base>(m, "ComponentLifeCycle")
                .def_prop_ro("is_started", &ComponentLifeCycle::is_started)
                .def_prop_ro("is_starting", &ComponentLifeCycle::is_starting)
                .def_prop_ro("is_stopping", &ComponentLifeCycle::is_stopping)
                .def("initialise", &initialise_component)
                .def("start", &start_component)
                .def("stop", &stop_component)
                .def("dispose", &dispose_component);

        m.def("initialise_component", &initialise_component, "component"_a);
        m.def("start_component", &start_component, "component"_a);
        m.def("stop_component", &stop_component, "component"_a);
        m.def("dispose_component", &dispose_component, "component"_a);

        nb::class_<InitialiseDisposeContext>(m, "initialise_dispose_context").def(
            nb::init<ComponentLifeCycle &>(), "component"_a);

        nb::class_<StartStopContext>(m, "start_stop_context").def(nb::init<ComponentLifeCycle &>(), "component"_a);
    }

    struct TransitionGuard {
        TransitionGuard(ComponentLifeCycle &component) : _component{component} { _component._transitioning = true; }
        ~TransitionGuard() { _component._transitioning = false; }

    private:
        ComponentLifeCycle &_component;
    };

    void initialise_component(ComponentLifeCycle &component) { component.initialise(); }

    /*
     * NOTE the LifeCycle methods are expected to be called on a single thread, so the simple gaurd clauses
     * used here are sufficient to ensure we don't accidentally start/stop more than once.
     */

    void start_component(ComponentLifeCycle &component) {
        if (component.is_started() || component.is_starting()) { return; }
        TransitionGuard guard{component};
        component.start();
        // If start fails (throws an exception), we will not land up setting the started flag to true.
        // But in either case (success or failure) the TransitionGuard destructor will be called and the
        // transitioning flag will be set to false.
        component._started = true;
    }

    void stop_component(ComponentLifeCycle &component) {
        if (!component.is_started() || component.is_stopping()) { return; }
        TransitionGuard guard{component};
        component.stop();
        component._started = false;
    }

    void dispose_component(ComponentLifeCycle &component) { component.dispose(); }

    InitialiseDisposeContext::InitialiseDisposeContext(ComponentLifeCycle &component) : _component{component} {
        initialise_component(component);
    }

    InitialiseDisposeContext::~InitialiseDisposeContext() noexcept {
        // Destructors must not throw. Ensure cleanup happens but never aborts the process.
        try {
            dispose_component(_component);
        } catch (const std::exception &e) {
            // Swallow exceptions to avoid std::terminate during stack unwinding.
            // The original operational exceptions should already have been raised during evaluation.
            // If cleanup fails, emit a warning to stderr to aid debugging without killing the interpreter.
            fprintf(stderr, "Warning: exception during dispose_component: %s\n", e.what());
        } catch (...) {
            fprintf(stderr, "Warning: unknown exception during dispose_component\n");
        }
    }

    StartStopContext::StartStopContext(ComponentLifeCycle &component) : _component{component} {
        start_component(_component);
    }

    StartStopContext::~StartStopContext() noexcept {
        // RAII finally pattern: exceptions from stop() should propagate to Python
        // But we can't throw from destructor during unwinding (causes std::terminate)
        // Solution: set Python error state instead of throwing C++ exception
        try {
            stop_component(_component);
        } catch (nb::python_error &e) {
            // Python exception from stop_fn - nanobind cleared it when throwing, so restore it
            e.restore();
        } catch (const std::exception &e) {
            // C++ exception from stop - convert to Python exception
            // Only set Python error if one isn't already set (preserve first exception)
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
            }
        } catch (...) {
            // Unknown exception
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_RuntimeError, "Unknown exception during stop_component");
            }
        }
    }
} // namespace hgraph