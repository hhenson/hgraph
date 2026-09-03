#include <hgraph/types/time_series/ts_output/list_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool tsl_output_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsl_output_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSOutputView tsl_output_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::size_t, TSOutputView> tsl_output_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            return {index, static_cast<const TSLOutputView *>(context)->at(index)};
        }

        // Structural-delta ranges are ordinal-indexed over a contiguous window
        // (RFC 0031), so each projector maps ordinal -> absolute index.
        [[nodiscard]] std::size_t tsl_output_added_index(const TSLOutputView *view, std::size_t ordinal)
        {
            return view->data_view().previous_size(view->evaluation_time()) + ordinal;
        }

        [[nodiscard]] std::size_t tsl_output_removed_index(const TSLOutputView *view, std::size_t ordinal)
        {
            return view->size() + ordinal;
        }

        [[nodiscard]] TSOutputView tsl_output_project_added(const void *context, const void *,
                                                            std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLOutputView *>(context);
            return view->retained_at(tsl_output_added_index(view, ordinal));
        }

        [[nodiscard]] TSOutputView tsl_output_project_removed(const void *context, const void *,
                                                              std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLOutputView *>(context);
            return view->retained_at(tsl_output_removed_index(view, ordinal));
        }

        [[nodiscard]] std::pair<std::size_t, TSOutputView> tsl_output_project_added_item(
            const void *context, const void *, std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLOutputView *>(context);
            const auto index = tsl_output_added_index(view, ordinal);
            return {index, view->retained_at(index)};
        }

        [[nodiscard]] std::pair<std::size_t, TSOutputView> tsl_output_project_removed_item(
            const void *context, const void *, std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLOutputView *>(context);
            const auto index = tsl_output_removed_index(view, ordinal);
            return {index, view->retained_at(index)};
        }

    }  // namespace

    TSLOutputView::TSLOutputView(TSOutputView view)
        : TSOutputTypedView<TSLOutputView>(std::move(view))
    {
        detail::validate_output_view_kind(schema(), TSTypeKind::TSL, "TSLOutputView");
    }

    TSLDataView TSLOutputView::data_view() const
    {
        return view_.data_view().as_list();
    }

    std::size_t TSLOutputView::size() const
    {
        return data_view().size();
    }

    bool TSLOutputView::empty() const
    {
        return data_view().empty();
    }

    Range<TSOutputView> TSLOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                   .projector = &tsl_output_project_value};
    }

    Range<TSOutputView> TSLOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsl_output_valid_child,
                                   .projector = &tsl_output_project_value};
    }

    Range<TSOutputView> TSLOutputView::modified_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsl_output_modified_child,
                                   .projector = &tsl_output_project_value};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = nullptr,
                                                        .projector = &tsl_output_project_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::valid_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = &tsl_output_valid_child,
                                                        .projector = &tsl_output_project_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::modified_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = &tsl_output_modified_child,
                                                        .projector = &tsl_output_project_item};
    }

    Range<std::size_t> TSLOutputView::added_indices() const
    {
        return data_view().added_indices(view_.evaluation_time());
    }

    Range<std::size_t> TSLOutputView::removed_indices() const
    {
        return data_view().removed_indices(view_.evaluation_time());
    }

    Range<TSOutputView> TSLOutputView::added_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr,
                                   .limit = added_indices().limit, .predicate = nullptr,
                                   .projector = &tsl_output_project_added};
    }

    Range<TSOutputView> TSLOutputView::removed_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr,
                                   .limit = removed_indices().limit, .predicate = nullptr,
                                   .projector = &tsl_output_project_removed};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::added_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = added_indices().limit,
                                                        .predicate = nullptr,
                                                        .projector = &tsl_output_project_added_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::removed_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = removed_indices().limit,
                                                        .predicate = nullptr,
                                                        .projector = &tsl_output_project_removed_item};
    }

    TSOutputView TSLOutputView::retained_at(std::size_t index) const
    {
        return TSOutputView{view_.output(), data_view().retained_at(index), view_.evaluation_time()};
    }

    void TSLOutputView::resize(std::size_t size) const
    {
        const auto evaluation_time = view_.evaluation_time();
        if (evaluation_time == MIN_DT)
        {
            throw std::invalid_argument("dynamic TSL resize requires a concrete evaluation time");
        }
        auto &base = view_.data_view();
        auto  list = base.as_list();
        if (!list.supports_resize())
        {
            throw std::logic_error("TSLOutputView::resize requires a dynamic TSL output");
        }
        if (list.size() == size) { return; }
        list.resize(size, evaluation_time);
        // A pure length change still ticks the list: the structural delta is
        // only visible while the storage window and the parent's modification
        // agree.
        auto mutation = base.begin_mutation(evaluation_time);
        mutation.mark_modified();
    }

    TSOutputView TSLOutputView::at(std::size_t index) &
    {
        auto &base = view_.data_view();
        if (schema() != nullptr && schema()->fixed_size() == 0 && index >= base.as_list().size())
        {
            if (view_.evaluation_time() == MIN_DT)
            {
                throw std::invalid_argument("dynamic TSL output growth requires a concrete evaluation time");
            }
            // Growth is a STRUCTURAL change: it goes through the timed resize
            // so the added indices are attributed to this cycle (RFC 0031).
            base.as_list().resize(index + 1, view_.evaluation_time());
        }
        return view_.indexed_child_at(index);
    }

    TSOutputView TSLOutputView::at(std::size_t index) const &
    {
        return const_cast<TSLOutputView *>(this)->at(index);
    }

    TSOutputView TSLOutputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSOutputView TSLOutputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

}  // namespace hgraph
