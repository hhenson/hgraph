#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/value/value.h>

namespace hgraph::value
{
    /**
     * The new value layer uses ordinary C++ constness instead of a separate
     * mutable-view hierarchy.
     *
     * `ValueView` is therefore retained as an alias to `View` so existing code
     * can keep its older naming while sharing the same implementation.
     */
    using ValueView = ::hgraph::View;
    using View = ::hgraph::View;
    using AtomicView = ::hgraph::AtomicView;
    using TupleView = ::hgraph::TupleView;
    using BundleView = ::hgraph::BundleView;
    using ListView = ::hgraph::ListView;
    using SetView = ::hgraph::SetView;
    using MapView = ::hgraph::MapView;
    using CyclicBufferView = ::hgraph::CyclicBufferView;
    using QueueView = ::hgraph::QueueView;
}  // namespace hgraph::value
