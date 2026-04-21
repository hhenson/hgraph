#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_builder.h>

#include <cassert>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

namespace hgraph
{
    TSOutputBuilder::TSOutputBuilder(const TSMeta &schema, const TSValueBuilder &ts_value_builder) noexcept
        : m_schema(schema), m_ts_value_builder(ts_value_builder)
    {
    }

    void *TSOutputBuilder::allocate() const
    {
        return ts_value_builder().allocate();
    }

    void TSOutputBuilder::construct(void *memory) const
    {
        ts_value_builder().construct(memory);
    }

    void TSOutputBuilder::destruct(void *memory) const noexcept
    {
        ts_value_builder().destruct(memory);
    }

    void TSOutputBuilder::deallocate(void *memory) const noexcept
    {
        ts_value_builder().deallocate(memory);
    }

    void TSOutputBuilder::copy_construct(void *dst, const void *src, const TSOutputBuilder &src_builder) const
    {
        if (!compatible_with(src_builder)) {
            throw std::invalid_argument("TSOutput copy construction requires matching builder");
        }
        ts_value_builder().copy_construct(dst, src, src_builder.ts_value_builder());
    }

    void TSOutputBuilder::move_construct(void *dst, void *src, const TSOutputBuilder &src_builder) const
    {
        if (!compatible_with(src_builder)) {
            throw std::invalid_argument("TSOutput move construction requires matching builder");
        }
        ts_value_builder().move_construct(dst, src, src_builder.ts_value_builder());
    }

    bool TSOutputBuilder::compatible_with(const TSOutputBuilder &other) const noexcept
    {
        return &m_schema.get() == &other.m_schema.get() && &m_ts_value_builder.get() == &other.m_ts_value_builder.get();
    }

    TSOutput TSOutputBuilder::make_output() const
    {
        return TSOutput{*this};
    }

    void TSOutputBuilder::construct_output(TSOutput &output, void *memory, MemoryOwnership ownership) const
    {
        assert(memory != nullptr);
        assert(output.m_builder == nullptr);
        assert(output.m_alternatives.empty());

        output.m_builder = this;
        output.rebind_builder(
            ts_value_builder(), ownership == MemoryOwnership::Owned ? TSValue::StorageOwnership::Owned : TSValue::StorageOwnership::External);
        try {
            construct(memory);
            output.attach_storage(memory);
        } catch (...) {
            output.m_builder = nullptr;
            output.detach_storage();
            output.reset_binding();
            throw;
        }
    }

    void TSOutputBuilder::construct_output(TSOutput &output) const
    {
        output.clear_storage();
        output.reset_binding();
        output.m_builder = nullptr;
        output.m_alternatives.clear();

        void *memory = allocate();
        try {
            construct_output(output, memory, MemoryOwnership::Owned);
        } catch (...) {
            deallocate(memory);
            throw;
        }
    }

    void TSOutputBuilder::copy_construct_output(TSOutput &output,
                                                const TSOutput &other,
                                                void *memory,
                                                MemoryOwnership ownership) const
    {
        if (other.m_builder != this || !compatible_with(other.builder())) {
            throw std::invalid_argument("TSOutputBuilder::copy_construct_output requires matching builder");
        }

        assert(memory != nullptr);
        assert(output.m_builder == nullptr);
        assert(output.m_alternatives.empty());

        output.m_builder = this;
        output.rebind_builder(
            ts_value_builder(), ownership == MemoryOwnership::Owned ? TSValue::StorageOwnership::Owned : TSValue::StorageOwnership::External);
        try {
            copy_construct(memory, other.storage_memory(), other.builder());
            output.attach_storage(memory);
            output.m_alternatives.clear();
        } catch (...) {
            output.m_alternatives.clear();
            output.m_builder = nullptr;
            output.detach_storage();
            output.reset_binding();
            throw;
        }
    }

    void TSOutputBuilder::copy_construct_output(TSOutput &output, const TSOutput &other) const
    {
        output.clear_storage();
        output.reset_binding();
        output.m_builder = nullptr;
        output.m_alternatives.clear();

        void *memory = allocate();
        try {
            copy_construct_output(output, other, memory, MemoryOwnership::Owned);
        } catch (...) {
            deallocate(memory);
            throw;
        }
    }

    void TSOutputBuilder::move_construct_output(TSOutput &output, TSOutput &other) const
    {
        if (other.m_builder != this || !compatible_with(other.builder())) {
            throw std::invalid_argument("TSOutputBuilder::move_construct_output requires matching builder");
        }

        output.clear_storage();
        output.reset_binding();
        output.m_builder = this;
        output.rebind_builder(ts_value_builder(),
                              other.owns_storage() ? TSValue::StorageOwnership::Owned : TSValue::StorageOwnership::External);
        output.m_storage = other.m_storage;
        output.m_alternatives.clear();
        if (BaseState *root_state = output.ts_root_state(); root_state != nullptr) {
            root_state->parent = &output;
        }

        other.m_builder = nullptr;
        other.m_alternatives.clear();
        other.reset_binding();
    }

    void TSOutputBuilder::destruct_output(TSOutput &output) const noexcept
    {
        if (output.m_builder != this) { return; }
        output.m_alternatives.clear();
        ts_value_builder().destruct_value(output);
        output.m_builder = nullptr;
    }

    const TSOutputBuilder *TSOutputBuilderFactory::builder_for(const TSMeta &schema)
    {
        static std::unordered_map<const TSMeta *, TSOutputBuilder> cache;
        static std::mutex mutex;

        std::lock_guard lock(mutex);
        if (const auto it = cache.find(&schema); it != cache.end()) { return &it->second; }

        const auto [it, inserted] = cache.emplace(&schema, TSOutputBuilder{schema, TSValueBuilderFactory::checked_builder_for(schema)});
        static_cast<void>(inserted);
        return &it->second;
    }

    const TSOutputBuilder *TSOutputBuilderFactory::builder_for(const TSMeta *schema)
    {
        return schema != nullptr ? builder_for(*schema) : nullptr;
    }

    const TSOutputBuilder &TSOutputBuilderFactory::checked_builder_for(const TSMeta &schema)
    {
        return *builder_for(schema);
    }

    const TSOutputBuilder &TSOutputBuilderFactory::checked_builder_for(const TSMeta *schema)
    {
        if (schema == nullptr) { throw std::invalid_argument("TSOutputBuilderFactory requires a non-null schema"); }
        return checked_builder_for(*schema);
    }
}  // namespace hgraph
