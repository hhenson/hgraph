#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/value/value.h>

namespace hgraph::value
{
    /**
     * The new value layer no longer models a shared `IndexedView` base.
     *
     * Callers should use the concrete collection view types directly. This
     * compatibility header preserves the include path and re-exports those
     * concrete types.
     */
    using TupleView = ::hgraph::TupleView;
    using BundleView = ::hgraph::BundleView;
    using ListView = ::hgraph::ListView;
    using SetView = ::hgraph::SetView;
    using MapView = ::hgraph::MapView;
    using CyclicBufferView = ::hgraph::CyclicBufferView;
    using QueueView = ::hgraph::QueueView;
}  // namespace hgraph::value
