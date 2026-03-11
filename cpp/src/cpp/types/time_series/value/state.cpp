#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/state.h>

#include <stdexcept>

namespace hgraph {

ValueBuilder::ValueBuilder(const value::TypeMeta &schema, const detail::StateOps &state_ops) noexcept
    : m_schema(schema), m_size(0), m_alignment(alignof(std::max_align_t)), m_state_ops(state_ops)
{
    state_ops.expand_builder(*this, schema);
}

void *ValueBuilder::allocate() const
{
    return ::operator new(m_size, std::align_val_t{m_alignment});
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

void ValueBuilder::destroy(void *memory) const noexcept
{
    m_state_ops.get().destroy(memory);
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

const ValueBuilder *ValueBuilderFactory::builder_for(const value::TypeMeta *schema) noexcept
{
    if (schema == nullptr || schema->kind != value::TypeKind::Atomic) {
        return nullptr;
    }

#define HGRAPH_VALUE_BUILDER_FACTORY_CASE(type_)                                                                                \
    if (schema == value::scalar_type_meta<type_>()) {                                                                           \
        return &atomic_builder<type_>();                                                                                        \
    }

    HGRAPH_VALUE_BUILDER_FACTORY_CASE(bool)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(int8_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(int16_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(int32_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(int64_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(uint8_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(uint16_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(uint32_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(uint64_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(size_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(float)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(double)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(std::string)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(engine_date_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(engine_time_t)
    HGRAPH_VALUE_BUILDER_FACTORY_CASE(engine_time_delta_t)

#undef HGRAPH_VALUE_BUILDER_FACTORY_CASE

    return nullptr;
}

const ValueBuilder &ValueBuilderFactory::checked_builder_for(const value::TypeMeta *schema)
{
    if (const ValueBuilder *builder = builder_for(schema); builder != nullptr) {
        return *builder;
    }
    throw std::runtime_error("ValueBuilderFactory: unsupported schema");
}

}  // namespace hgraph
