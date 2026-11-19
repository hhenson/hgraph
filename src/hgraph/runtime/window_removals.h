#pragma once

#include <hgraph/util/date_time.h>

namespace hgraph
{
    struct TSParentLink;
    struct WindowRemovalRegistration;

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
