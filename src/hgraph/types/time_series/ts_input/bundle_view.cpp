#include <hgraph/types/time_series/ts_input/bundle_view.h>

#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] std::string_view tsb_input_project_key(const void *context, const void *, std::size_t index)
        {
            const auto *view = static_cast<const TSBInputView *>(context);
            const auto &ops = detail::input_endpoint_ops_for(view->schema());
            return ops.key_at != nullptr ? ops.key_at(view->schema(), index) : std::string_view{};
        }

        [[nodiscard]] bool tsb_input_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBInputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsb_input_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBInputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSInputView tsb_input_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBInputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::string_view, TSInputView> tsb_input_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            const auto *view = static_cast<const TSBInputView *>(context);
            const auto &ops = detail::input_endpoint_ops_for(view->schema());
            const auto key = ops.key_at != nullptr ? ops.key_at(view->schema(), index) : std::string_view{};
            return {key, view->at(index)};
        }

    }  // namespace

    TSBInputView::TSBInputView(TSInputView view)
        : TSInputTypedView<TSBInputView>(std::move(view))
    {
        detail::validate_input_view_kind(schema(), TSTypeKind::TSB, "TSBInputView");
    }

    std::size_t TSBInputView::size() const
    {
        const auto &ops = detail::input_endpoint_ops_for(schema());
        return ops.child_count != nullptr ? ops.child_count(schema()) : 0;
    }

    bool TSBInputView::empty() const
    {
        return size() == 0;
    }

    bool TSBInputView::has_field(std::string_view name) const noexcept
    {
        return find_field_index(name) != npos;
    }

    TSBDataView TSBInputView::data_view() const
    {
        return view_.data_view().as_bundle();
    }

    Range<std::string_view> TSBInputView::keys() const &
    {
        return Range<std::string_view>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                       .projector = &tsb_input_project_key};
    }

    Range<TSInputView> TSBInputView::values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                  .projector = &tsb_input_project_value};
    }

    Range<TSInputView> TSBInputView::valid_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsb_input_valid_child,
                                  .projector = &tsb_input_project_value};
    }

    Range<TSInputView> TSBInputView::modified_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsb_input_modified_child,
                                  .projector = &tsb_input_project_value};
    }

    KeyValueRange<std::string_view, TSInputView> TSBInputView::items() const &
    {
        return KeyValueRange<std::string_view, TSInputView>{.context = this,
                                                            .memory = nullptr,
                                                            .limit = size(),
                                                            .predicate = nullptr,
                                                            .projector = &tsb_input_project_item};
    }

    KeyValueRange<std::string_view, TSInputView> TSBInputView::valid_items() const &
    {
        return KeyValueRange<std::string_view, TSInputView>{.context = this,
                                                            .memory = nullptr,
                                                            .limit = size(),
                                                            .predicate = &tsb_input_valid_child,
                                                            .projector = &tsb_input_project_item};
    }

    KeyValueRange<std::string_view, TSInputView> TSBInputView::modified_items() const &
    {
        return KeyValueRange<std::string_view, TSInputView>{.context = this,
                                                            .memory = nullptr,
                                                            .limit = size(),
                                                            .predicate = &tsb_input_modified_child,
                                                            .projector = &tsb_input_project_item};
    }

    TSInputView TSBInputView::at(std::size_t index) &
    {
        if (index >= size()) { throw std::out_of_range("TSBInputView::at index out of range"); }
        if (view_.is_target_position())
        {
            const auto &data = view_.data_view();
            if (detail::has_input_children(data))
            {
                // A from-REF alternative behind the position: project like an
                // input so the per-child LINK tracking (sampled rebinds)
                // reaches modified()/delta reads.
                return view_.child_from_resolved_input(data, index);
            }
            auto bundle = data.as_bundle();
            return view_.child_from_target(bundle.at(index), index);
        }
        return view_.child_from_input(index);
    }

    TSInputView TSBInputView::at(std::size_t index) const &
    {
        return const_cast<TSBInputView *>(this)->at(index);
    }

    TSInputView TSBInputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSInputView TSBInputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSInputView TSBInputView::at(std::string_view name) &
    {
        return at(field_index(name));
    }

    TSInputView TSBInputView::at(std::string_view name) const &
    {
        return const_cast<TSBInputView *>(this)->at(name);
    }

    TSInputView TSBInputView::field(std::string_view name) &
    {
        return at(name);
    }

    TSInputView TSBInputView::field(std::string_view name) const &
    {
        return at(name);
    }

    TSInputView TSBInputView::operator[](std::string_view name) &
    {
        return at(name);
    }

    TSInputView TSBInputView::operator[](std::string_view name) const &
    {
        return at(name);
    }

    std::size_t TSBInputView::field_index(std::string_view name) const
    {
        const auto index = find_field_index(name);
        if (index == npos) { throw std::out_of_range("TSBInputView field not found"); }
        return index;
    }

    std::size_t TSBInputView::find_field_index(std::string_view name) const noexcept
    {
        return fallback_on_exception(npos, [&] {
            const auto &ops = detail::input_endpoint_ops_for(schema());
            return ops.find_key != nullptr ? ops.find_key(schema(), name) : npos;
        });
    }

    std::string_view TSBInputView::key_at(std::size_t index) const noexcept
    {
        return fallback_on_exception(std::string_view{}, [&] {
            const auto &ops = detail::input_endpoint_ops_for(schema());
            return ops.key_at != nullptr ? ops.key_at(schema(), index) : std::string_view{};
        });
    }

}  // namespace hgraph
