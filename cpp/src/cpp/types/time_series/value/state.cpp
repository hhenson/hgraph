#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/state.h>

#include <stdexcept>

namespace hgraph
{

    ValueBuilder::ValueBuilder(const value::TypeMeta &schema, const detail::StateOps &state_ops) noexcept
        : m_schema(schema),
          m_state_ops(state_ops),
          m_view_dispatch(state_ops.view_dispatch(schema))
    {
        state_ops.expand_builder(*this, schema);
    }

    void *ValueBuilder::allocate() const
    {
        return ::operator new(m_size, std::align_val_t{m_alignment});
    }

    void ValueBuilder::destroy(void *memory) const noexcept
    {
        m_state_ops.get().destroy(memory);
    }

    void ValueBuilder::deallocate(void *memory) const noexcept
    {
        if (memory != nullptr) {
            ::operator delete(memory, std::align_val_t{m_alignment});
        }
    }

    void ValueBuilder::construct(void *memory) const
    {
        m_state_ops.get().construct(memory);
    }

    void ValueBuilder::copy_construct(void *dst, const void *src, const ValueBuilder &src_builder) const
    {
        if (this != &src_builder) {
            throw std::invalid_argument("ValueBuilder::copy_construct requires matching builder");
        }
        m_state_ops.get().copy_construct(dst, src);
    }

    void ValueBuilder::move_construct(void *dst, void *src, const ValueBuilder &src_builder) const
    {
        if (this != &src_builder) {
            throw std::invalid_argument("ValueBuilder::move_construct requires matching builder");
        }
        m_state_ops.get().move_construct(dst, src);
    }

    const ValueBuilder *ValueBuilderFactory::builder_for(const value::TypeMeta *schema)
    {
        if (schema == nullptr) { return nullptr; }

        switch (schema->kind) {
        case value::TypeKind::Atomic:
            return detail::atomic_builder_for(schema);
        case value::TypeKind::List:
            return detail::list_builder_for(schema);
        default:
            return nullptr;
        }
    }

    const ValueBuilder &ValueBuilderFactory::checked_builder_for(const value::TypeMeta *schema)
    {
        if (const ValueBuilder *builder = builder_for(schema); builder != nullptr) {
            return *builder;
        }
        throw std::runtime_error("ValueBuilderFactory: unsupported schema");
    }

}  // namespace hgraph
