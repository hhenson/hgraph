#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/state.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

namespace hgraph {

namespace detail
{

    struct FixedListStateOps final : StateOps
    {
        FixedListStateOps(const ValueBuilder &element_builder, size_t fixed_size) noexcept
            : m_element_builder(element_builder), m_fixed_size(fixed_size)
        {
        }

        void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            builder.cache_layout(
                FixedListState::allocation_size(m_element_builder.get(), m_fixed_size),
                FixedListState::allocation_alignment(m_element_builder.get()));
            builder.cache_lifecycle(true, true);
        }

        [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            return true;
        }

        [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            return true;
        }

        void construct(void *memory) const override
        {
            std::construct_at(state(memory), m_element_builder.get().schema(), m_fixed_size);
            state(memory)->construct_elements(m_element_builder.get());
        }

        void destroy(void *memory) const noexcept override
        {
            state(memory)->destroy_elements(m_element_builder.get());
            std::destroy_at(state(memory));
        }

        void copy_construct(void *dst, const void *src) const override
        {
            std::construct_at(state(dst), state(src)->element_schema(), m_fixed_size);
            state(dst)->copy_from(*state(src), m_element_builder.get());
        }

        void move_construct(void *dst, void *src) const override
        {
            std::construct_at(state(dst), state(src)->element_schema(), m_fixed_size);
            state(dst)->move_from(*state(src), m_element_builder.get());
        }

      private:
        [[nodiscard]] static FixedListState *state(void *memory) noexcept
        {
            return std::launder(reinterpret_cast<FixedListState *>(memory));
        }

        [[nodiscard]] static const FixedListState *state(const void *memory) noexcept
        {
            return std::launder(reinterpret_cast<const FixedListState *>(memory));
        }

        std::reference_wrapper<const ValueBuilder> m_element_builder;
        size_t                                     m_fixed_size;
    };

    struct DynamicListStateOps final : StateOps
    {
        explicit DynamicListStateOps(const ValueBuilder &element_builder) noexcept
            : m_element_builder(element_builder)
        {
        }

        void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            builder.cache_layout(sizeof(DynamicListState), alignof(DynamicListState));
            builder.cache_lifecycle(true, true);
        }

        [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            return true;
        }

        [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            return true;
        }

        void construct(void *memory) const override
        {
            std::construct_at(state(memory), m_element_builder.get().schema());
        }

        void destroy(void *memory) const noexcept override
        {
            state(memory)->clear(m_element_builder.get());
            std::destroy_at(state(memory));
        }

        void copy_construct(void *dst, const void *src) const override
        {
            std::construct_at(state(dst), state(src)->element_schema());
            state(dst)->copy_from(*state(src), m_element_builder.get());
        }

        void move_construct(void *dst, void *src) const override
        {
            std::construct_at(state(dst), state(src)->element_schema());
            state(dst)->move_from(*state(src));
        }

      private:
        [[nodiscard]] static DynamicListState *state(void *memory) noexcept
        {
            return std::launder(reinterpret_cast<DynamicListState *>(memory));
        }

        [[nodiscard]] static const DynamicListState *state(const void *memory) noexcept
        {
            return std::launder(reinterpret_cast<const DynamicListState *>(memory));
        }

        std::reference_wrapper<const ValueBuilder> m_element_builder;
    };

    struct CachedBuilderEntry
    {
        std::unique_ptr<StateOps>   state_ops;
        std::unique_ptr<ValueBuilder> builder;
    };

}  // namespace detail

ValueBuilder::ValueBuilder(const value::TypeMeta &schema, const detail::StateOps &state_ops) noexcept
    : m_schema(schema),
      m_size(0),
      m_alignment(alignof(std::max_align_t)),
      m_requires_destroy(state_ops.requires_destroy(schema)),
      m_requires_deallocate(state_ops.requires_deallocate(schema)),
      m_state_ops(state_ops)
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

const ValueBuilder *ValueBuilderFactory::builder_for(const value::TypeMeta *schema)
{
    if (schema == nullptr) {
        return nullptr;
    }

    if (schema->kind == value::TypeKind::Atomic) {
#define HGRAPH_VALUE_BUILDER_FACTORY_CASE(type_)                                                                                \
        if (schema == value::scalar_type_meta<type_>()) {                                                                       \
            return &atomic_builder<type_>();                                                                                    \
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

    if (schema->kind == value::TypeKind::List) {
        static std::mutex cache_mutex;
        static std::unordered_map<const value::TypeMeta *, detail::CachedBuilderEntry> cache;

        std::lock_guard lock(cache_mutex);
        if (auto it = cache.find(schema); it != cache.end()) {
            return it->second.builder.get();
        }

        if (schema->element_type == nullptr) {
            throw std::runtime_error("ValueBuilderFactory: list schema requires an element schema");
        }

        const ValueBuilder &element_builder = checked_builder_for(schema->element_type);

        detail::CachedBuilderEntry entry;
        if (schema->is_fixed_size()) {
            entry.state_ops = std::make_unique<detail::FixedListStateOps>(element_builder, schema->fixed_size);
        } else {
            entry.state_ops = std::make_unique<detail::DynamicListStateOps>(element_builder);
        }
        entry.builder = std::make_unique<ValueBuilder>(*schema, *entry.state_ops);

        auto [it, inserted] = cache.emplace(schema, std::move(entry));
        static_cast<void>(inserted);
        return it->second.builder.get();
    }

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
