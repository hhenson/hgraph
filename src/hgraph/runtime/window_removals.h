#pragma once

#include "root_evaluation_step.h"

#include <memory>

namespace hgraph
{
    struct TSParentLink;
    struct WindowRemovalRegistration;

    // A ring slot can outlive the graph which published it (for example, when
    // endpoint storage moves). Stop keeps the last step readable. Restart and
    // destruction detach the run before its inspection state can be reused.
    struct WindowRemovalClock
    {
        const RootStepInspection *step{};
        bool closed(DateTime when) const noexcept
        {
            return !step || when < step->time || (when == step->time &&
                (step->status == StepInspection::Released || step->status == StepInspection::Releasing));
        }
    };
    void observe_window_removal(const TSParentLink &parent, std::shared_ptr<const WindowRemovalClock> &clock);

    // The graph owns registrations. A moving or destroyed storage updates its
    // registration before relinquishing its address. All work is root-thread local.
    // Moving the queue's dense storage only updates the window's registration.
    // A null registration delivered by the queue retires the complete change.
    using SetWindowRemovalRegistration = void (*)(void *, WindowRemovalRegistration *) noexcept;
    WindowRemovalRegistration *track_window_removals(const TSParentLink &parent, DateTime time,
                                                      void *window, SetWindowRemovalRegistration set_registration);
    void cancel_window_removals(WindowRemovalRegistration *registration) noexcept;
    void relocate_window_removals(WindowRemovalRegistration *registration, void *window) noexcept;

    bool window_removals_closed(const TSParentLink &parent, DateTime when);
}
