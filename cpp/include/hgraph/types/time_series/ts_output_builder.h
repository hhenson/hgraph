#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_value_builder.h>

#include <functional>

namespace hgraph
{
    struct TSOutput;

    /**
     * Cached schema-bound builder for `TSOutput`.
     *
     * `TSOutput` currently owns only one `TSValue` storage region, so this
     * builder is intentionally thin. It still owns the object-level lifecycle
     * surface for outputs so endpoint construction follows the same pattern as
     * `TSInput`.
     */
    struct HGRAPH_EXPORT TSOutputBuilder
    {
        enum class MemoryOwnership : uint8_t
        {
            External,
            Owned = 1,
        };

        [[nodiscard]] const TSMeta &schema() const noexcept { return m_schema.get(); }
        [[nodiscard]] const TSValueBuilder &ts_value_builder() const noexcept { return m_ts_value_builder.get(); }
        [[nodiscard]] size_t size() const noexcept { return ts_value_builder().size(); }
        [[nodiscard]] size_t alignment() const noexcept { return ts_value_builder().alignment(); }

        [[nodiscard]] void *allocate() const;
        void construct(void *memory) const;
        void destruct(void *memory) const noexcept;
        void deallocate(void *memory) const noexcept;
        void copy_construct(void *dst, const void *src, const TSOutputBuilder &src_builder) const;
        void move_construct(void *dst, void *src, const TSOutputBuilder &src_builder) const;

        [[nodiscard]] bool compatible_with(const TSOutputBuilder &other) const noexcept;
        [[nodiscard]] TSOutput make_output() const;
        void construct_output(TSOutput &output, void *memory, MemoryOwnership ownership = MemoryOwnership::External) const;
        void construct_output(TSOutput &output) const;
        void copy_construct_output(TSOutput &output,
                                   const TSOutput &other,
                                   void *memory,
                                   MemoryOwnership ownership = MemoryOwnership::External) const;
        void copy_construct_output(TSOutput &output, const TSOutput &other) const;
        void move_construct_output(TSOutput &output, TSOutput &other) const;
        void destruct_output(TSOutput &output) const noexcept;

      private:
        friend struct TSOutputBuilderFactory;

        TSOutputBuilder(const TSMeta &schema, const TSValueBuilder &ts_value_builder) noexcept;

        std::reference_wrapper<const TSMeta> m_schema;
        std::reference_wrapper<const TSValueBuilder> m_ts_value_builder;
    };

    struct HGRAPH_EXPORT TSOutputBuilderFactory
    {
        [[nodiscard]] static const TSOutputBuilder *builder_for(const TSMeta &schema);
        [[nodiscard]] static const TSOutputBuilder *builder_for(const TSMeta *schema);
        [[nodiscard]] static const TSOutputBuilder &checked_builder_for(const TSMeta &schema);
        [[nodiscard]] static const TSOutputBuilder &checked_builder_for(const TSMeta *schema);
    };
}  // namespace hgraph
