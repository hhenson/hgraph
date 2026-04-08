#pragma once

#include <hgraph/hgraph_export.h>

#include <cstddef>

namespace hgraph::detail
{
    /**
     * Observer for stable-slot lifecycle events in delta-tracked associative storage.
     *
     * The value layer owns authoritative slot allocation, liveness, and reuse.
     * Higher layers can subscribe to those events to keep parallel runtime state
     * exactly synchronized with structural mutations.
     */
    struct HGRAPH_EXPORT SlotObserver
    {
        virtual ~SlotObserver() = default;

        virtual void on_capacity(size_t old_capacity, size_t new_capacity) = 0;
        virtual void on_insert(size_t slot) = 0;
        /**
         * A previously live slot became logically removed.
         *
         * The slot may still remain occupied so removed key/value payloads
         * stay inspectable until the next outer mutation begins or the slot is
         * otherwise physically erased.
         */
        virtual void on_remove(size_t slot) = 0;
        /**
         * A previously occupied slot was physically erased or released for
         * reuse.
         *
         * After this callback the observer must no longer assume removed
         * payloads or per-slot state for @p slot still exist.
         */
        virtual void on_erase(size_t slot) = 0;
        virtual void on_clear() = 0;
    };
}  // namespace hgraph::detail
