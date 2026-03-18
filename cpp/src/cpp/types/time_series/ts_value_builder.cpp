#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_value_builder.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] size_t align_up(size_t value, size_t alignment) noexcept
        {
            const size_t mask = alignment - 1;
            return (value + mask) & ~mask;
        }

        template <typename TState>
        TState make_initialized_root_state()
        {
            TState state{};
            state.parent = static_cast<TSOutput *>(nullptr);
            state.index = 0;
            state.last_modified_time = MIN_DT;
            return state;
        }

        [[nodiscard]] TimeSeriesStateV make_root_state(const TSMeta &schema)
        {
            switch (schema.kind) {
                case TSKind::TSValue: return make_initialized_root_state<TSState>();
                case TSKind::TSS: return make_initialized_root_state<TSSState>();
                case TSKind::TSD: return make_initialized_root_state<TSDState>();
                case TSKind::TSL: return make_initialized_root_state<TSLState>();
                case TSKind::TSW: return make_initialized_root_state<TSWState>();
                case TSKind::TSB: return make_initialized_root_state<TSBState>();
                case TSKind::REF: return make_initialized_root_state<RefLinkState>();
                case TSKind::SIGNAL: return make_initialized_root_state<SignalState>();
            }

            throw std::invalid_argument("TSValueBuilder requires a supported TS schema kind");
        }

        struct RootTSStateOps : detail::TSBuilderOps
        {
            [[nodiscard]] detail::TSBuilderLayout layout(const TSMeta &schema,
                                                         const ValueBuilder &value_builder) const noexcept override
            {
                static_cast<void>(schema);

                const size_t value_offset = 0;
                const size_t value_size = value_builder.size();
                const size_t value_alignment = value_builder.alignment();
                const size_t ts_alignment = alignof(TimeSeriesStateV);
                const size_t ts_offset = align_up(value_size, ts_alignment);
                const size_t total_size = ts_offset + sizeof(TimeSeriesStateV);
                const size_t total_alignment = std::max(value_alignment, ts_alignment);
                return detail::TSBuilderLayout{
                    .value_offset = value_offset,
                    .ts_offset = ts_offset,
                    .size = total_size,
                    .alignment = total_alignment,
                };
            }

            void construct(void *memory, const TSMeta &schema) const override
            {
                new (memory) TimeSeriesStateV(make_root_state(schema));
            }

            void destruct(void *memory) const noexcept override
            {
                std::destroy_at(static_cast<TimeSeriesStateV *>(memory));
            }

            void copy_construct(void *dst, const void *src, const TSMeta &schema) const override
            {
                static_cast<void>(schema);
                new (dst) TimeSeriesStateV(*static_cast<const TimeSeriesStateV *>(src));
            }

            void move_construct(void *dst, void *src, const TSMeta &schema) const override
            {
                static_cast<void>(schema);
                new (dst) TimeSeriesStateV(std::move(*static_cast<TimeSeriesStateV *>(src)));
            }
        };

        [[nodiscard]] const detail::TSBuilderOps &root_builder_ops() noexcept
        {
            static RootTSStateOps ops;
            return ops;
        }
    }  // namespace

    TSValueBuilder::TSValueBuilder(const TSMeta &schema,
                                   const ValueBuilder &value_builder,
                                   const detail::TSBuilderOps &builder_ops) noexcept
        : m_schema(schema), m_value_builder(value_builder), m_builder_ops(builder_ops)
    {
        const auto layout = builder_ops.layout(schema, value_builder);
        m_value_offset = layout.value_offset;
        m_ts_offset = layout.ts_offset;
        m_size = layout.size;
        m_alignment = layout.alignment;
    }

    void *TSValueBuilder::allocate() const
    {
        return ::operator new(m_size, std::align_val_t{m_alignment});
    }

    void TSValueBuilder::deallocate(void *memory) const noexcept
    {
        ::operator delete(memory, std::align_val_t{m_alignment});
    }

    void TSValueBuilder::construct(void *memory) const
    {
        value_builder().construct(value_memory(memory));
        try {
            m_builder_ops.get().construct(ts_memory(memory), schema());
        } catch (...) {
            value_builder().destruct(value_memory(memory));
            throw;
        }
    }

    void TSValueBuilder::destruct(void *memory) const noexcept
    {
        m_builder_ops.get().destruct(ts_memory(memory));
        value_builder().destruct(value_memory(memory));
    }

    void TSValueBuilder::copy_construct(void *dst, const void *src, const TSValueBuilder &src_builder) const
    {
        if (this != &src_builder) { throw std::invalid_argument("TSValue copy construction requires matching builder"); }

        value_builder().copy_construct(value_memory(dst), src_builder.value_memory(src), value_builder());
        try {
            m_builder_ops.get().copy_construct(ts_memory(dst), src_builder.ts_memory(src), schema());
        } catch (...) {
            value_builder().destruct(value_memory(dst));
            throw;
        }
    }

    void TSValueBuilder::move_construct(void *dst, void *src, const TSValueBuilder &src_builder) const
    {
        if (this != &src_builder) { throw std::invalid_argument("TSValue move construction requires matching builder"); }

        value_builder().move_construct(value_memory(dst), src_builder.value_memory(src), value_builder());
        try {
            m_builder_ops.get().move_construct(ts_memory(dst), src_builder.ts_memory(src), schema());
        } catch (...) {
            value_builder().destruct(value_memory(dst));
            throw;
        }
    }

    void *TSValueBuilder::value_memory(void *memory) const noexcept
    {
        return static_cast<std::byte *>(memory) + m_value_offset;
    }

    const void *TSValueBuilder::value_memory(const void *memory) const noexcept
    {
        return static_cast<const std::byte *>(memory) + m_value_offset;
    }

    void *TSValueBuilder::ts_memory(void *memory) const noexcept
    {
        return static_cast<std::byte *>(memory) + m_ts_offset;
    }

    const void *TSValueBuilder::ts_memory(const void *memory) const noexcept
    {
        return static_cast<const std::byte *>(memory) + m_ts_offset;
    }

    const TSValueBuilder *TSValueBuilderFactory::builder_for(const TSMeta &schema)
    {
        return builder_for(&schema);
    }

    const TSValueBuilder *TSValueBuilderFactory::builder_for(const TSMeta *schema)
    {
        if (schema == nullptr) { return nullptr; }
        if (schema->value_type == nullptr) { return nullptr; }

        static std::unordered_map<const TSMeta *, TSValueBuilder> cache;
        static std::recursive_mutex mutex;

        std::lock_guard lock(mutex);
        if (const auto it = cache.find(schema); it != cache.end()) { return &it->second; }

        const ValueBuilder &value_builder = ValueBuilderFactory::checked_builder_for(schema->value_type, MutationTracking::Delta);
        const auto [it, inserted] = cache.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(schema),
            std::forward_as_tuple(std::cref(*schema), std::cref(value_builder), std::cref(root_builder_ops())));
        static_cast<void>(inserted);
        return &it->second;
    }

    const TSValueBuilder &TSValueBuilderFactory::checked_builder_for(const TSMeta &schema)
    {
        return checked_builder_for(&schema);
    }

    const TSValueBuilder &TSValueBuilderFactory::checked_builder_for(const TSMeta *schema)
    {
        if (const auto *builder = builder_for(schema); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSValueBuilderFactory requires a schema with a value_type");
    }
}  // namespace hgraph
