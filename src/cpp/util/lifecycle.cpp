#include <hgraph/util/lifecycle.h>

namespace hgraph {
    bool ComponentLifeCycle::is_initialised() const {
        return _initialised;
    }

    bool ComponentLifeCycle::is_initialising() const {
        return _initialised_transitioning && !_initialised;
    }

    bool ComponentLifeCycle::is_disposing() const {
        return _initialised_transitioning && _initialised;
    }

    bool ComponentLifeCycle::is_started() const {
        return _started;
    }

    bool ComponentLifeCycle::is_starting() const {
        return _started_transitioning && !_started;
    }

    bool ComponentLifeCycle::is_stopping() const {
        return _started_transitioning && _started;
    }

    InitialiseTransitionGuard::InitialiseTransitionGuard(ComponentLifeCycle &component_, bool initialised_) : component{
            component_
        },
        initialised{initialised_} {
        component._initialised_transitioning = true;
    }

    InitialiseTransitionGuard::~InitialiseTransitionGuard() {
        component._initialised_transitioning = false;
        component._initialised = initialised;
    }

    StartTransitionGuard::StartTransitionGuard(ComponentLifeCycle &component_, bool started_) : component{component_},
        started{started_} {
        component._started_transitioning = true;
    }

    StartTransitionGuard::~StartTransitionGuard() {
        component._started_transitioning = false;
        component._started = started;
    }

    void initialise_component(ComponentLifeCycle &component) {
        if (component.is_initialised() || component.is_initialising()) { return; }
        InitialiseTransitionGuard guard{component, true};
        component.initialise();
    }

    void start_component(ComponentLifeCycle &component) {
        if (component.is_started() || component.is_starting()) { return; }
        StartTransitionGuard guard{component, true};
        component.start();
    }

    void stop_component(ComponentLifeCycle &component) {
        if (!component.is_started() || component.is_stopping()) { return; }
        StartTransitionGuard guard{component, false};
        component.stop();
    }

    void dispose_component(ComponentLifeCycle &component) {
        if (!component.is_initialised() || component.is_disposing()) { return; }
        InitialiseTransitionGuard guard{component, false};
        component.dispose();
    }
}
