
#pragma once

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct Notifiable {
        virtual ~Notifiable() = default;

        virtual void notify(engine_time_t et) = 0;

        // Fired by `~BaseState()` on every remaining subscriber just before
        // the state's memory is reclaimed. Default is a no-op so existing
        // forward subscribers (RootRefValueNotifier, WrappedRefNotifier, etc.)
        // need no changes. Subclasses that hold a raw pointer to the state
        // override this to clear that pointer.
        virtual void notify_invalidated() noexcept {}
    };
}
