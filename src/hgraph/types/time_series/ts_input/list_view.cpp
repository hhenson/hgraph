#include <hgraph/types/time_series/ts_input/list_view.h>

#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool tsl_input_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLInputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsl_input_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLInputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSInputView tsl_input_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLInputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::size_t, TSInputView> tsl_input_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            return {index, static_cast<const TSLInputView *>(context)->at(index)};
        }

        // Structural-delta ranges are ordinal-indexed over a contiguous window
        // (RFC 0031), so each projector maps ordinal -> absolute index.
        [[nodiscard]] std::size_t tsl_input_added_index(const TSLInputView *view, std::size_t ordinal)
        {
            return view->data_view().previous_size(view->evaluation_time()) + ordinal;
        }

        [[nodiscard]] std::size_t tsl_input_removed_index(const TSLInputView *view, std::size_t ordinal)
        {
            return view->size() + ordinal;
        }

        [[nodiscard]] TSInputView tsl_input_project_added(const void *context, const void *,
                                                           std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLInputView *>(context);
            return view->retained_at(tsl_input_added_index(view, ordinal));
        }

        [[nodiscard]] TSInputView tsl_input_project_removed(const void *context, const void *,
                                                             std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLInputView *>(context);
            return view->retained_at(tsl_input_removed_index(view, ordinal));
        }

        [[nodiscard]] std::pair<std::size_t, TSInputView> tsl_input_project_added_item(
            const void *context, const void *, std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLInputView *>(context);
            const auto index = tsl_input_added_index(view, ordinal);
            return {index, view->retained_at(index)};
        }

        [[nodiscard]] std::pair<std::size_t, TSInputView> tsl_input_project_removed_item(
            const void *context, const void *, std::size_t ordinal)
        {
            const auto *view = static_cast<const TSLInputView *>(context);
            const auto index = tsl_input_removed_index(view, ordinal);
            return {index, view->retained_at(index)};
        }

    }  // namespace

    TSLInputView::TSLInputView(TSInputView view)
        : TSInputTypedView<TSLInputView>(std::move(view))
    {
        detail::validate_input_view_kind(schema(), TSTypeKind::TSL, "TSLInputView");
    }

    std::size_t TSLInputView::size() const
    {
        if (view_.is_target_position())
        {
            const auto &data = view_.data_view();
            if (data.valid()) { return data.as_list().size(); }
        }
        const auto &ops = detail::input_endpoint_ops_for(schema());
        return ops.child_count != nullptr ? ops.child_count(schema()) : 0;
    }

    bool TSLInputView::empty() const
    {
        return size() == 0;
    }

    TSLDataView TSLInputView::data_view() const
    {
        return view_.data_view().as_list();
    }

    Range<TSInputView> TSLInputView::values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                  .projector = &tsl_input_project_value};
    }

    Range<TSInputView> TSLInputView::valid_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsl_input_valid_child,
                                  .projector = &tsl_input_project_value};
    }

    Range<TSInputView> TSLInputView::modified_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsl_input_modified_child,
                                  .projector = &tsl_input_project_value};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::items() const &
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = size(),
                                                       .predicate = nullptr,
                                                       .projector = &tsl_input_project_item};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::valid_items() const &
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = size(),
                                                       .predicate = &tsl_input_valid_child,
                                                       .projector = &tsl_input_project_item};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::modified_items() const &
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = size(),
                                                       .predicate = &tsl_input_modified_child,
                                                       .projector = &tsl_input_project_item};
    }

    Range<std::size_t> TSLInputView::added_indices() const &
    {
        if (!view_.is_target_position()) { return {}; }
        const auto &data = view_.data_view();
        if (!data.valid()) { return {}; }
        return data.as_list().added_indices(view_.evaluation_time());
    }

    Range<std::size_t> TSLInputView::removed_indices() const &
    {
        if (!view_.is_target_position()) { return {}; }
        const auto &data = view_.data_view();
        if (!data.valid()) { return {}; }
        return data.as_list().removed_indices(view_.evaluation_time());
    }

    Range<TSInputView> TSLInputView::added_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr,
                                  .limit = added_indices().limit, .predicate = nullptr,
                                  .projector = &tsl_input_project_added};
    }

    Range<TSInputView> TSLInputView::removed_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr,
                                  .limit = removed_indices().limit, .predicate = nullptr,
                                  .projector = &tsl_input_project_removed};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::added_items() const &
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = added_indices().limit,
                                                       .predicate = nullptr,
                                                       .projector = &tsl_input_project_added_item};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::removed_items() const &
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = removed_indices().limit,
                                                       .predicate = nullptr,
                                                       .projector = &tsl_input_project_removed_item};
    }

    TSInputView TSLInputView::retained_at(std::size_t index) const &
    {
        if (index < size()) { return const_cast<TSLInputView *>(this)->at(index); }
        const auto &data = view_.data_view();
        if (!data.valid())
        {
            throw std::out_of_range("TSLInputView::retained_at index out of range");
        }
        return view_.child_from_retained(data.as_list().retained_at(index));
    }

    TSInputView TSLInputView::at(std::size_t index) &
    {
        if (index >= size()) { throw std::out_of_range("TSLInputView::at index out of range"); }
        if (view_.is_target_position())
        {
            const auto &data = view_.data_view();
            if (detail::has_input_children(data))
            {
                // A from-REF alternative owns per-child input links. Preserve
                // their sampled rebind tracking when projecting the list.
                return view_.child_from_resolved_input(data, index);
            }
            auto list = data.as_list();
            return view_.child_from_target(list.at(index), index);
        }
        return view_.child_from_input(index);
    }

    TSInputView TSLInputView::at(std::size_t index) const &
    {
        return const_cast<TSLInputView *>(this)->at(index);
    }

    TSInputView TSLInputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSInputView TSLInputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

}  // namespace hgraph
