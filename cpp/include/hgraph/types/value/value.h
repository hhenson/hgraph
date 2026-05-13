#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/sequence.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

namespace hgraph::value
{
    /**
     * The canonical value implementation now lives in the new time-series
     * value layer.
     *
     * This header keeps the long-standing `hgraph::value::*` include path and
     * names stable while routing all runtime code through the newer plain-data
     * value/view implementation.
     */
    using MutationTracking = ::hgraph::MutationTracking;
    using View = ::hgraph::View;
    using ValueView = ::hgraph::View;
    using AtomicView = ::hgraph::AtomicView;
    using TupleView = ::hgraph::TupleView;
    using BundleView = ::hgraph::BundleView;
    using ListView = ::hgraph::ListView;
    using SetView = ::hgraph::SetView;
    using MapView = ::hgraph::MapView;
    using CyclicBufferView = ::hgraph::CyclicBufferView;
    using QueueView = ::hgraph::QueueView;
    using Value = ::hgraph::Value;

    template <typename T>
    [[nodiscard]] inline Value value_for(T &&value)
    {
        return ::hgraph::value_for(std::forward<T>(value));
    }

    template <typename T>
    [[nodiscard]] inline Value value_for(const TypeMeta &schema, T &&value)
    {
        return ::hgraph::value_for(schema, std::forward<T>(value));
    }

}  // namespace hgraph::value
