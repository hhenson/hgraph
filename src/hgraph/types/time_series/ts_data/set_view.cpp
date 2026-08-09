#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <vector>

namespace hgraph
{
    TSSDataView::TSSDataView(TSDataView view)
        : storage_(view.storage_ref(), TSTypeKind::TSS)
    {
    }

    TSDataView TSSDataView::base() const noexcept
    {
        return TSDataView{storage_.storage_ref()};
    }

    const TSValueTypeMetaData *TSSDataView::schema() const noexcept
    {
        return base().schema();
    }

    const TSSDataLayout &TSSDataView::layout() const
    {
        return static_cast<const TSSDataLayout &>(base().layout());
    }

    ValueView TSSDataView::value() const
    {
        return base().value();
    }

    ValueView TSSDataView::delta_value(DateTime evaluation_time) const
    {
        return base().delta_value(evaluation_time);
    }

    DateTime TSSDataView::last_modified_time() const
    {
        return base().last_modified_time();
    }

    bool TSSDataView::modified(DateTime evaluation_time) const
    {
        return base().modified(evaluation_time);
    }

    void TSSDataView::subscribe(Notifiable *observer) const
    {
        base().subscribe(observer);
    }

    void TSSDataView::unsubscribe(Notifiable *observer) const
    {
        base().unsubscribe(observer);
    }

    bool TSSDataView::has_observers() const
    {
        return base().has_observers();
    }

    std::size_t TSSDataView::observer_count() const
    {
        return base().observer_count();
    }

    void TSSDataView::subscribe_slot_observer(SlotObserver *observer) const
    {
        const auto &ops = set_ops();
        ops.subscribe_slot_observer_impl(ops.context, storage_.data(), observer);
    }

    void TSSDataView::unsubscribe_slot_observer(SlotObserver *observer) const
    {
        const auto &ops = set_ops();
        ops.unsubscribe_slot_observer_impl(ops.context, storage_.data(), observer);
    }

    std::size_t TSSDataView::size() const
    {
        const auto &ops = set_ops();
        return ops.size_impl(ops.context, storage_.data());
    }

    bool TSSDataView::empty() const
    {
        return size() == 0;
    }

    std::size_t TSSDataView::slot_capacity() const
    {
        const auto &ops = set_ops();
        return ops.slot_capacity_impl(ops.context, storage_.data());
    }

    bool TSSDataView::slot_occupied(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_occupied_impl(ops.context, storage_.data(), slot);
    }

    bool TSSDataView::slot_live(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_live_impl(ops.context, storage_.data(), slot);
    }

    bool TSSDataView::slot_added(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_added_impl(ops.context, storage_.data(), slot);
    }

    bool TSSDataView::slot_removed(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_removed_impl(ops.context, storage_.data(), slot);
    }

    std::size_t TSSDataView::next_added_slot(std::size_t previous) const
    {
        const auto &ops = set_ops();
        return ops.next_added_slot_impl(ops.context, storage_.data(), previous);
    }

    std::size_t TSSDataView::next_removed_slot(std::size_t previous) const
    {
        const auto &ops = set_ops();
        return ops.next_removed_slot_impl(ops.context, storage_.data(), previous);
    }

    ValueView TSSDataView::at_slot(std::size_t slot) const
    {
        if (!slot_occupied(slot)) { throw std::out_of_range("TSSDataView::at_slot: slot is not occupied"); }
        const auto &ops = set_ops();
        return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, storage_.data(), slot)};
    }

    bool TSSDataView::contains(const ValueView &key) const
    {
        const auto &ops = set_ops();
        return ops.contains_impl(ops.context, storage_.data(), key);
    }

    std::size_t TSSDataView::find_slot(const ValueView &key) const
    {
        const auto &ops = set_ops();
        return ops.find_slot_impl(ops.context, storage_.data(), key);
    }

    Range<ValueView> TSSDataView::values() const
    {
        const auto &ops = set_ops();
        return ops.make_values_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSSDataView::added() const
    {
        const auto &ops = set_ops();
        return ops.make_added_values_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSSDataView::removed() const
    {
        const auto &ops = set_ops();
        return ops.make_removed_values_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSSDataView::added_values() const
    {
        return added();
    }

    Range<ValueView> TSSDataView::removed_values() const
    {
        return removed();
    }

    Range<ValueView>::iterator TSSDataView::begin() const
    {
        return values().begin();
    }

    Range<ValueView>::iterator TSSDataView::end() const
    {
        return values().end();
    }

    TSSDataMutationView TSSDataView::begin_mutation(DateTime evaluation_time) const
    {
        return TSSDataMutationView{base(), evaluation_time};
    }

    const TSSDataOps &TSSDataView::set_ops() const
    {
        return storage_.ops();
    }

    TSSDataMutationView::TSSDataMutationView(TSDataView view, DateTime evaluation_time)
        : TSSDataView(TSDataView{view.storage_ref()}),
          mutation_(view.begin_mutation(evaluation_time))
    {
        if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSS)
        {
            throw std::invalid_argument("TSSDataMutationView requires a TSS TSData kind");
        }
    }

    TSSDataMutationView::TSSDataMutationView(TSSDataMutationView &&) noexcept = default;

    TSSDataMutationView::~TSSDataMutationView() noexcept = default;

    TSSDataView TSSDataMutationView::view()
    {
        return TSSDataView{base()};
    }

    DateTime TSSDataMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    void TSSDataMutationView::reserve(std::size_t capacity)
    {
        const auto &ops = set_ops();
        ops.reserve_impl(ops.context, mutation_.mutable_data(), capacity);
    }

    void TSSDataMutationView::touch()
    {
        const auto &ops = set_ops();
        if (ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
    }

    bool TSSDataMutationView::add(const ValueView &key)
    {
        const auto &ops    = set_ops();
        const auto  result = ops.insert_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        if (!result.changed && ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
        return result.changed;
    }

    bool TSSDataMutationView::remove(const ValueView &key)
    {
        const auto &ops    = set_ops();
        const auto  result = ops.remove_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        if (!result.changed && ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
        return result.changed;
    }

    void TSSDataMutationView::clear()
    {
        std::vector<Value> keys;
        for (const auto key : values()) { keys.emplace_back(key); }
        const auto &ops           = set_ops();
        const bool  newly_touched = ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time());
        for (const auto &key : keys) { static_cast<void>(remove(key.view())); }
        if (newly_touched) { mutation_.mark_modified(); }
    }

    bool TSSDataMutationView::copy_value_from(const ValueView &source)
    {
        return mutation_.copy_value_from(source);
    }
}  // namespace hgraph
