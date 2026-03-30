#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/time_series/value/value.h>

#include <stdexcept>

namespace hgraph
{

    ValueBuilder::ValueBuilder(const value::TypeMeta &schema, MutationTracking tracking, const detail::ValueBuilderOps &state_ops) noexcept
        : m_schema(schema),
          m_tracking(tracking),
          m_builder_ops(state_ops),
          m_view_dispatch(state_ops.view_dispatch(schema))
    {
        const detail::BuilderLayout layout = state_ops.layout(schema);
        m_size = layout.size;
        m_alignment = layout.alignment;
        m_requires_destruct = state_ops.requires_destruct(schema);
        m_requires_deallocate = state_ops.requires_deallocate(schema);
        m_stores_inline_in_value_handle = state_ops.stores_inline_in_value_handle(schema);
    }
    const value::TypeMeta &ValueBuilder::schema() const noexcept { return m_schema.get(); }
    size_t                 ValueBuilder::size() const noexcept { return m_size; }
    size_t                 ValueBuilder::alignment() const noexcept { return m_alignment; }
    MutationTracking       ValueBuilder::tracking() const noexcept { return m_tracking; }
    bool                   ValueBuilder::requires_destruct() const noexcept { return m_requires_destruct; }
    bool                   ValueBuilder::requires_deallocate() const noexcept { return m_requires_deallocate; }
    bool                   ValueBuilder::stores_inline_in_value_handle() const noexcept { return m_stores_inline_in_value_handle; }
    const detail::ViewDispatch &ValueBuilder::dispatch() const noexcept { return m_view_dispatch.get(); }
    Value ValueBuilder::make_value() const
    {
        Value value;
        construct_value(value);
        return value;
    }

    void ValueBuilder::construct_value(Value &value) const
    {
        if (stores_inline_in_value_handle()) {
            construct(static_cast<void *>(value.m_storage.inline_storage.data()));
            value.m_builder = this;
            return;
        }

        void *memory = allocate();
        try {
            construct(memory);
            value.m_storage.heap_memory = memory;
            value.m_builder = this;
        } catch (...) {
            deallocate(memory);
            throw;
        }
    }

    void ValueBuilder::copy_construct_value(Value &value, const Value &other) const
    {
        if (other.m_builder == nullptr || &other.builder() != this) {
            throw std::invalid_argument("ValueBuilder::copy_construct_value requires matching builder");
        }

        if (stores_inline_in_value_handle()) {
            copy_construct(static_cast<void *>(value.m_storage.inline_storage.data()), other.storage_memory(), other.builder());
            value.m_builder = this;
            return;
        }

        if (!other.has_value()) {
            value.m_builder = this;
            value.m_storage.heap_memory = nullptr;
            return;
        }

        void *memory = allocate();
        try {
            copy_construct(memory, other.storage_memory(), other.builder());
            value.m_storage.heap_memory = memory;
            value.m_builder = this;
        } catch (...) {
            deallocate(memory);
            throw;
        }
    }

    void ValueBuilder::move_construct_value(Value &value, Value &other) const
    {
        if (other.m_builder == nullptr || &other.builder() != this) {
            throw std::invalid_argument("ValueBuilder::move_construct_value requires matching builder");
        }

        if (stores_inline_in_value_handle()) {
            move_construct(static_cast<void *>(value.m_storage.inline_storage.data()), other.storage_memory(), other.builder());
            value.m_builder = this;
            return;
        }

        value.m_storage.heap_memory = other.m_storage.heap_memory;
        value.m_builder = this;
        other.m_storage.heap_memory = nullptr;
    }

    void ValueBuilder::destruct_value(Value &value) const noexcept
    {
        if (value.m_builder != this || !value.has_value()) { return; }

        if (requires_destruct()) {
            destruct(stores_inline_in_value_handle() ? static_cast<void *>(value.m_storage.inline_storage.data()) : value.m_storage.heap_memory);
        }
        if (requires_deallocate() && !stores_inline_in_value_handle()) {
            deallocate(value.m_storage.heap_memory);
            value.m_storage.heap_memory = nullptr;
        }
    }

    void *ValueBuilder::allocate() const
    {
        return ::operator new(m_size, std::align_val_t{m_alignment});
    }

    void ValueBuilder::destruct(void *memory) const noexcept
    {
        m_builder_ops.get().destruct(memory);
    }

    void ValueBuilder::deallocate(void *memory) const noexcept
    {
        if (memory != nullptr) {
            ::operator delete(memory, std::align_val_t{m_alignment});
        }
    }

    void ValueBuilder::construct(void *memory) const
    {
        m_builder_ops.get().construct(memory);
    }

    void ValueBuilder::copy_construct(void *dst, const void *src, const ValueBuilder &src_builder) const
    {
        if (this != &src_builder) {
            throw std::invalid_argument("ValueBuilder::copy_construct requires matching builder");
        }
        m_builder_ops.get().copy_construct(dst, src);
    }

    void ValueBuilder::move_construct(void *dst, void *src, const ValueBuilder &src_builder) const
    {
        if (this != &src_builder) {
            throw std::invalid_argument("ValueBuilder::move_construct requires matching builder");
        }
        m_builder_ops.get().move_construct(dst, src);
    }

    const ValueBuilder *ValueBuilderFactory::builder_for(const value::TypeMeta *schema, MutationTracking tracking)
    {
        if (schema == nullptr) { return nullptr; }

        switch (schema->kind) {
        case value::TypeKind::Atomic:
            return detail::atomic_builder_for(schema, tracking);
        case value::TypeKind::Tuple:
        case value::TypeKind::Bundle:
            return detail::record_builder_for(schema, tracking);
        case value::TypeKind::List:
            return detail::list_builder_for(schema, tracking);
        case value::TypeKind::Set:
        case value::TypeKind::Map:
            return detail::associative_builder_for(schema, tracking);
        case value::TypeKind::CyclicBuffer:
        case value::TypeKind::Queue:
            return detail::sequence_builder_for(schema, tracking);
        default:
            return nullptr;
        }
    }

    const ValueBuilder &ValueBuilderFactory::checked_builder_for(const value::TypeMeta *schema, MutationTracking tracking)
    {
        if (const ValueBuilder *builder = builder_for(schema, tracking); builder != nullptr) {
            return *builder;
        }
        throw std::runtime_error("ValueBuilderFactory: unsupported schema");
    }

}  // namespace hgraph
