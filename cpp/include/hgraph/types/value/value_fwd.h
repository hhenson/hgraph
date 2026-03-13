#pragma once

#include <cstddef>
#include <cstdint>

namespace hgraph
{
    struct View;
    struct Value;
    struct AtomicView;
    struct TupleView;
    struct BundleView;
    struct ListView;
    struct SetView;
    struct MapView;
    struct CyclicBufferView;
    struct QueueView;
}

namespace hgraph::value
{
    enum class TypeKind : uint8_t;
    enum class TypeFlags : uint32_t;
    struct TypeMeta;
    struct type_ops;
    struct BundleFieldInfo;
    class TypeRegistry;

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
    const TypeMeta *scalar_type_meta();
}  // namespace hgraph::value
