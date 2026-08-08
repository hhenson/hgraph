#include <hgraph/types/time_series/ts_output/dict_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool tsd_output_live_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDOutputView *>(context)->slot_live(slot);
        }

        [[nodiscard]] bool tsd_output_valid_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_live(slot) && view->at_slot(slot).valid();
        }

        [[nodiscard]] bool tsd_output_modified_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_live(slot) && view->slot_modified(slot);
        }

        [[nodiscard]] bool tsd_output_added_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_occupied(slot) && view->slot_added(slot);
        }

        [[nodiscard]] bool tsd_output_removed_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_occupied(slot) && view->slot_removed(slot);
        }

        [[nodiscard]] TSOutputView tsd_output_project_value(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDOutputView *>(context)->at_slot(slot);
        }

        [[nodiscard]] std::pair<ValueView, TSOutputView> tsd_output_project_item(
            const void *context,
            const void *,
            std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return {view->key_at_slot(slot), view->at_slot(slot)};
        }
    }  // namespace

    TSDOutputView::TSDOutputView(TSOutputView view)
        : TSOutputTypedView<TSDOutputView>(std::move(view))
    {
        detail::validate_output_view_kind(schema(), TSTypeKind::TSD, "TSDOutputView");
    }

    TSDDataView TSDOutputView::data_view() const
    {
        return view_.data_view().as_dict();
    }

    std::size_t TSDOutputView::size() const { return data_view().size(); }
    bool TSDOutputView::empty() const { return data_view().empty(); }
    std::size_t TSDOutputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSDOutputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSDOutputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSDOutputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSDOutputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    bool TSDOutputView::slot_modified(std::size_t slot) const { return data_view().slot_modified(slot); }
    ValueView TSDOutputView::key_at_slot(std::size_t slot) const { return data_view().key_at_slot(slot); }
    TSOutputView TSDOutputView::at_slot(std::size_t slot) const
    {
        return TSOutputView{view_.output(), data_view().at_slot(slot), view_.evaluation_time()};
    }
    bool TSDOutputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSDOutputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }
    TSOutputView TSDOutputView::at(const ValueView &key) const
    {
        return TSOutputView{view_.output(), data_view().at(key), view_.evaluation_time()};
    }
    TSOutputView TSDOutputView::operator[](const ValueView &key) const { return at(key); }
    Range<ValueView> TSDOutputView::keys() const { return data_view().keys(); }

    Range<TSOutputView> TSDOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_live_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::items() const
    {
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_live_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::valid_keys() const { return data_view().valid_keys(); }

    Range<TSOutputView> TSDOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_valid_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::valid_items() const
    {
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_valid_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::modified_keys() const { return data_view().modified_keys(view_.evaluation_time()); }

    Range<TSOutputView> TSDOutputView::modified_values() const
    {
        if (!modified()) { return detail::empty_output_range<TSOutputView>(); }
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_modified_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::modified_items() const
    {
        if (!modified()) { return detail::empty_output_kv_range<ValueView, TSOutputView>(); }
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_modified_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::added_keys() const
    {
        if (!modified()) { return detail::empty_output_range<ValueView>(); }
        return data_view().added_keys();
    }

    Range<TSOutputView> TSDOutputView::added_values() const
    {
        if (!modified()) { return detail::empty_output_range<TSOutputView>(); }
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_added_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::added_items() const
    {
        if (!modified()) { return detail::empty_output_kv_range<ValueView, TSOutputView>(); }
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_added_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::removed_keys() const
    {
        if (!modified()) { return detail::empty_output_range<ValueView>(); }
        return data_view().removed_keys();
    }

    Range<TSOutputView> TSDOutputView::removed_values() const
    {
        if (!modified()) { return detail::empty_output_range<TSOutputView>(); }
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_removed_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::removed_items() const
    {
        if (!modified()) { return detail::empty_output_kv_range<ValueView, TSOutputView>(); }
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_removed_slot,
                                                      .projector = &tsd_output_project_item};
    }

    TSOutputView TSDOutputView::key_set() const
    {
        return TSOutputView{view_.output(), data_view().key_set().base(), view_.evaluation_time()};
    }

    TSDDataMutationView TSDOutputView::begin_mutation(DateTime evaluation_time) const
    {
        return data_view().begin_mutation(evaluation_time);
    }

}  // namespace hgraph
