#include <hgraph/types/time_series/ts_input/dict_view.h>

#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool tsd_input_live_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDInputView *>(context)->slot_live(slot);
        }

        [[nodiscard]] bool tsd_input_valid_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_live(slot) && view->at_slot(slot).valid();
        }

        [[nodiscard]] std::size_t tsd_input_modified_seek(const void *context, const void *, std::size_t first)
        {
            const auto &self = *static_cast<const TSDInputView *>(context);
            return self.data_view().next_modified_slot(first == 0 ? TS_DATA_NO_CHILD_ID : first - 1);
        }

        [[nodiscard]] bool tsd_input_modified_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_live(slot) && view->slot_modified(slot);
        }

        [[nodiscard]] std::size_t tsd_input_added_seek(const void *context, const void *, std::size_t first)
        {
            const auto &self = *static_cast<const TSDInputView *>(context);
            return self.data_view().next_membership_added_slot(first == 0 ? TS_DATA_NO_CHILD_ID : first - 1);
        }

        [[nodiscard]] bool tsd_input_added_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_occupied(slot) && view->slot_added(slot);
        }

        [[nodiscard]] ValueView tsd_input_project_key(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDInputView *>(context)->key_at_slot(slot);
        }

        [[nodiscard]] TSInputView tsd_input_project_value(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDInputView *>(context)->at_slot(slot);
        }

        [[nodiscard]] std::pair<ValueView, TSInputView> tsd_input_project_item(
            const void *context,
            const void *,
            std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return {view->key_at_slot(slot), view->at_slot(slot)};
        }
    }  // namespace

    TSDInputView::TSDInputView(TSInputView view)
        : TSInputTypedView<TSDInputView>(std::move(view))
    {
        detail::validate_input_view_kind(schema(), TSTypeKind::TSD, "TSDInputView");
    }

    TSDDataView TSDInputView::data_view() const
    {
        auto data = view_.input_data_view();
        return data.as_dict();
    }

    std::size_t TSDInputView::size() const { return data_view().size(); }
    bool TSDInputView::empty() const { return data_view().empty(); }
    std::size_t TSDInputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSDInputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSDInputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSDInputView::slot_added(std::size_t slot) const
    {
        return view_.inherited_sampled_transition()
                   ? slot_live(slot)
                   : structure_modified() && data_view().membership_slot_added(slot);
    }
    bool TSDInputView::slot_removed(std::size_t slot) const
    {
        return !view_.inherited_sampled_transition() && structure_modified() &&
               data_view().membership_slot_removed(slot);
    }
    bool TSDInputView::slot_modified(std::size_t slot) const
    {
        return view_.inherited_sampled_transition() ? slot_live(slot) : data_view().slot_modified(slot);
    }

    bool TSDInputView::structure_modified() const
    {
        if (view_.sampled_structural_transition()) { return true; }
        return data_view().structural_delta_current(view_.evaluation_time());
    }

    ValueView TSDInputView::key_at_slot(std::size_t slot) const { return data_view().key_at_slot(slot); }

    TSInputView TSDInputView::at_slot(std::size_t slot) const
    {
        auto child = data_view().at_slot(slot);
        return view_.child_from_target(std::move(child), slot);
    }

    bool TSDInputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSDInputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }

    TSInputView TSDInputView::at(const ValueView &key) const
    {
        const auto slot = find_slot(key);
        if (slot == TS_DATA_NO_CHILD_ID) { return TSInputView{}; }
        return at_slot(slot);
    }

    TSInputView TSDInputView::operator[](const ValueView &key) const
    {
        return at(key);
    }

    Range<ValueView> TSDInputView::keys() const & { return data_view().keys(); }

    Range<TSInputView> TSDInputView::values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_live_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::items() const &
    {
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_live_slot,
                                                     .projector = &tsd_input_project_item};
    }

    Range<ValueView> TSDInputView::valid_keys() const & { return data_view().valid_keys(); }

    Range<TSInputView> TSDInputView::valid_values() const &
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_valid_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::valid_items() const &
    {
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_valid_slot,
                                                     .projector = &tsd_input_project_item};
    }

    Range<ValueView> TSDInputView::modified_keys() const &
    {
        if (!modified()) { return detail::empty_input_range<ValueView>(); }
        if (!view_.inherited_sampled_transition()) { return data_view().modified_keys(view_.evaluation_time()); }
        return Range<ValueView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                .predicate = &tsd_input_modified_slot,
                                .projector = &tsd_input_project_key};
    }

    Range<TSInputView> TSDInputView::modified_values() const &
    {
        if (!modified()) { return detail::empty_input_range<TSInputView>(); }
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_modified_slot,
                                  .projector = &tsd_input_project_value,
                                  .seek = view_.inherited_sampled_transition() ? nullptr : &tsd_input_modified_seek};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::modified_items() const &
    {
        if (!modified()) { return detail::empty_input_kv_range<ValueView, TSInputView>(); }
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_modified_slot,
                                                     .projector = &tsd_input_project_item,
                                  .seek = view_.inherited_sampled_transition() ? nullptr : &tsd_input_modified_seek};
    }

    Range<ValueView> TSDInputView::added_keys() const &
    {
        if (!structure_modified()) { return detail::empty_input_range<ValueView>(); }
        return Range<ValueView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                .predicate = &tsd_input_added_slot, .projector = &tsd_input_project_key,
                                  .seek = view_.inherited_sampled_transition() ? nullptr : &tsd_input_added_seek};
    }

    Range<TSInputView> TSDInputView::added_values() const &
    {
        if (!structure_modified()) { return detail::empty_input_range<TSInputView>(); }
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_added_slot,
                                  .projector = &tsd_input_project_value,
                                  .seek = view_.inherited_sampled_transition() ? nullptr : &tsd_input_added_seek};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::added_items() const &
    {
        if (!structure_modified()) { return detail::empty_input_kv_range<ValueView, TSInputView>(); }
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_added_slot,
                                                     .projector = &tsd_input_project_item,
                                  .seek = view_.inherited_sampled_transition() ? nullptr : &tsd_input_added_seek};
    }

    Range<ValueView> TSDInputView::removed_keys() const &
    {
        if (!structure_modified() || view_.inherited_sampled_transition()) { return detail::empty_input_range<ValueView>(); }
        const auto source = data_view().removed_items();
        return Range<ValueView>{.context = this, .memory = nullptr, .limit = source.limit,
            .predicate = [](const void *context, const void *, std::size_t slot) {
                const auto range = static_cast<const TSDInputView *>(context)->data_view().removed_items();
                return range.predicate == nullptr || range.predicate(range.context, range.memory, slot);
            }, .projector = [](const void *context, const void *, std::size_t slot) {
                const auto range = static_cast<const TSDInputView *>(context)->data_view().removed_items();
                return range.projector(range.context, range.memory, slot).first;
            }, .seek = [](const void *context, const void *, std::size_t first) {
                const auto range = static_cast<const TSDInputView *>(context)->data_view().removed_items();
                return range.seek == nullptr ? first : range.seek(range.context, range.memory, first);
            }};
    }

    Range<TSInputView> TSDInputView::removed_values() const &
    {
        if (!structure_modified() || view_.inherited_sampled_transition())
        {
            return detail::empty_input_range<TSInputView>();
        }
        const auto source = data_view().removed_values();
        return Range<TSInputView>{
            .context = this, .memory = nullptr, .limit = source.limit,
            .predicate = [](const void *context, const void *, std::size_t slot) {
                const auto &self = *static_cast<const TSDInputView *>(context);
                const auto data = self.data_view();
                const auto range = data.removed_values();
                return range.predicate == nullptr || range.predicate(range.context, range.memory, slot);
            },
            .projector = [](const void *context, const void *, std::size_t slot) {
                const auto &self = *static_cast<const TSDInputView *>(context);
                const auto range = self.data_view().removed_values();
                return self.view_.child_from_retained(range.projector(range.context, range.memory, slot));
            }, .seek = [](const void *context, const void *, std::size_t first) {
                const auto range = static_cast<const TSDInputView *>(context)->data_view().removed_values();
                return range.seek == nullptr ? first : range.seek(range.context, range.memory, first);
            }};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::removed_items() const &
    {
        if (!structure_modified() || view_.inherited_sampled_transition())
        {
            return detail::empty_input_kv_range<ValueView, TSInputView>();
        }
        const auto source = data_view().removed_items();
        return KeyValueRange<ValueView, TSInputView>{
            .context = this, .memory = nullptr, .limit = source.limit,
            .predicate = [](const void *context, const void *, std::size_t slot) {
                const auto &self = *static_cast<const TSDInputView *>(context);
                const auto data = self.data_view();
                const auto range = data.removed_items();
                return range.predicate == nullptr || range.predicate(range.context, range.memory, slot);
            },
            .projector = [](const void *context, const void *, std::size_t slot) {
                const auto &self = *static_cast<const TSDInputView *>(context);
                const auto range = self.data_view().removed_items();
                auto item = range.projector(range.context, range.memory, slot);
                return std::pair<ValueView, TSInputView>{std::move(item.first),
                    self.view_.child_from_retained(std::move(item.second))};
            }, .seek = [](const void *context, const void *, std::size_t first) {
                const auto range = static_cast<const TSDInputView *>(context)->data_view().removed_items();
                return range.seek == nullptr ? first : range.seek(range.context, range.memory, first);
            }};
    }

}  // namespace hgraph
