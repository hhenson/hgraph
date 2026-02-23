#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {

void LinkTarget::ActiveNotifier::notify(engine_time_t et) {
    if (owning_input) {
        owning_input->notify(et);
    }
}

LinkTarget::~LinkTarget() {
    // Remove ourselves from the output's observer list before dying.
    // This prevents ObserverList::~ObserverList() from calling on_source_destroyed()
    // on our already-freed memory (bidirectional cleanup).
    if (is_linked && observer_data) {
        auto* obs = static_cast<ObserverList*>(observer_data);
        if (obs->is_alive()) {
            obs->remove_observer(this);
            if (active_notifier.owning_input != nullptr) {
                obs->remove_observer(&active_notifier);
            }
        }
    }
    cleanup_ref_binding();
}

void LinkTarget::on_source_destroyed() {
    // Save dying_obs and owning_input before clearing fields — needed by ref_binding_osd_fn_
    void* dying_obs = observer_data;
    TSInput* saved_owning_input = active_notifier.owning_input;

    is_linked = false;
    value_data = nullptr;
    time_data = nullptr;
    observer_data = nullptr;
    delta_data = nullptr;
    link_data = nullptr;
    ops = nullptr;
    meta = nullptr;
    active_notifier.owning_input = nullptr;
    last_notify_time = MIN_DT;

    // If a REF binding exists, attempt immediate rebind to the current REF target.
    // This handles the case where the resolved target is destroyed (e.g., switch
    // changes key set) while the REF source still points to a valid new target.
    // Pass saved_owning_input so rebind can restore it and re-subscribe active_notifier.
    if (ref_binding_ && ref_binding_osd_fn_) {
        ref_binding_osd_fn_(ref_binding_, dying_obs, owner_time_ptr, saved_owning_input);
    }
}

} // namespace hgraph
