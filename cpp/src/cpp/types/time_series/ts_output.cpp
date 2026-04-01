#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>

#include <utility>

namespace hgraph
{
    namespace detail
    {
        namespace
        {
            struct DefaultTSOutputViewOps final : TSOutputViewOps
            {
                [[nodiscard]] LinkedTSContext linked_context(const TSViewContext &context) const noexcept override
                {
                    const TSViewContext resolved = context.resolved();
                    return LinkedTSContext{
                        resolved.schema,
                        resolved.value_dispatch,
                        resolved.ts_dispatch,
                        resolved.value_data,
                        context.ts_state,
                    };
                }
            };
        }  // namespace

        const TSOutputViewOps &default_output_view_ops() noexcept
        {
            static DefaultTSOutputViewOps ops;
            return ops;
        }
    }  // namespace detail

    TSOutput::TSOutput(const TSOutputBuilder &builder)
    {
        builder.construct_output(*this);
    }

    TSOutput::TSOutput(const TSOutput &other)
    {
        if (other.m_builder != nullptr) { other.builder().copy_construct_output(*this, other); }
    }

    TSOutput::TSOutput(TSOutput &&other) noexcept
    {
        if (other.m_builder != nullptr) { other.builder().move_construct_output(*this, other); }
    }

    TSOutput &TSOutput::operator=(const TSOutput &other)
    {
        if (this == &other) { return *this; }
        TSOutput replacement(other);
        return *this = std::move(replacement);
    }

    TSOutput &TSOutput::operator=(TSOutput &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        if (other.m_builder == nullptr) {
            m_builder = nullptr;
            m_alternatives.clear();
            return *this;
        }
        other.builder().move_construct_output(*this, other);
        return *this;
    }

    TSOutput::~TSOutput()
    {
        clear_storage();
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time)
    {
        TSViewContext context = view_context();
        context.output_view_ops = &detail::default_output_view_ops();
        return TSOutputView{context, TSViewContext::none(), evaluation_time};
    }

    TSValue *TSOutput::find_alternative(const TSMeta *schema) noexcept
    {
        const auto it = m_alternatives.find(schema);
        return it != m_alternatives.end() ? &it->second : nullptr;
    }

    const TSValue *TSOutput::find_alternative(const TSMeta *schema) const noexcept
    {
        const auto it = m_alternatives.find(schema);
        return it != m_alternatives.end() ? &it->second : nullptr;
    }

    TSValue &TSOutput::ensure_alternative(const TSMeta *schema)
    {
        return m_alternatives.try_emplace(schema, *schema).first->second;
    }

    void TSOutput::remove_alternative(const TSMeta *schema) noexcept
    {
        m_alternatives.erase(schema);
    }

    void TSOutput::clear_storage() noexcept
    {
        if (m_builder == nullptr) {
            m_alternatives.clear();
            return;
        }
        builder().destruct_output(*this);
    }
}  // namespace hgraph
