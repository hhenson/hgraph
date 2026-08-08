#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    TSDDataView::TSDDataView(TSDataView view)
        : storage_(view.storage_ref(), TSTypeKind::TSD)
    {
    }

    TSDataView TSDDataView::base() const noexcept
    {
        return TSDataView{storage_.storage_ref()};
    }

    const TSValueTypeMetaData *TSDDataView::schema() const noexcept
    {
        return base().schema();
    }

    const TSDDataLayout &TSDDataView::layout() const
    {
        return static_cast<const TSDDataLayout &>(base().layout());
    }

    ValueView TSDDataView::value() const
    {
        return base().value();
    }

    ValueView TSDDataView::delta_value(DateTime evaluation_time) const
    {
        return base().delta_value(evaluation_time);
    }

    DateTime TSDDataView::last_modified_time() const
    {
        return base().last_modified_time();
    }

    bool TSDDataView::modified(DateTime evaluation_time) const
    {
        return base().modified(evaluation_time);
    }

    void TSDDataView::subscribe(Notifiable *observer) const
    {
        base().subscribe(observer);
    }

    void TSDDataView::unsubscribe(Notifiable *observer) const
    {
        base().unsubscribe(observer);
    }

    bool TSDDataView::has_observers() const
    {
        return base().has_observers();
    }

    std::size_t TSDDataView::observer_count() const
    {
        return base().observer_count();
    }

    std::size_t TSDDataView::size() const
    {
        const auto &ops = dict_ops();
        return ops.size_impl(ops.context, storage_.data());
    }

    bool TSDDataView::empty() const
    {
        return size() == 0;
    }

    std::size_t TSDDataView::slot_capacity() const
    {
        const auto &ops = dict_ops();
        return ops.slot_capacity_impl(ops.context, storage_.data());
    }

    bool TSDDataView::slot_occupied(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_occupied_impl(ops.context, storage_.data(), slot);
    }

    bool TSDDataView::slot_live(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_live_impl(ops.context, storage_.data(), slot);
    }

    bool TSDDataView::slot_added(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_added_impl(ops.context, storage_.data(), slot);
    }

    bool TSDDataView::slot_removed(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_removed_impl(ops.context, storage_.data(), slot);
    }

    std::size_t TSDDataView::next_added_slot(std::size_t previous) const
    {
        const auto &ops = dict_ops();
        return ops.next_added_slot_impl(ops.context, storage_.data(), previous);
    }

    std::size_t TSDDataView::next_removed_slot(std::size_t previous) const
    {
        const auto &ops = dict_ops();
        return ops.next_removed_slot_impl(ops.context, storage_.data(), previous);
    }

    bool TSDDataView::slot_modified(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_modified_impl(ops.context, storage_.data(), slot);
    }

    std::size_t TSDDataView::next_modified_slot(std::size_t previous) const
    {
        const auto &ops = dict_ops();
        return ops.next_modified_slot_impl(ops.context, storage_.data(), previous);
    }

    ValueView TSDDataView::key_at_slot(std::size_t slot) const
    {
        if (!slot_occupied(slot)) { throw std::out_of_range("TSDDataView::key_at_slot: slot is not occupied"); }
        const auto &ops = dict_ops();
        return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, storage_.data(), slot)};
    }

    ValueView TSDDataView::removed_key_at_slot(std::size_t slot) const
    {
        if (!slot_removed(slot))
        {
            throw std::out_of_range("TSDDataView::removed_key_at_slot: slot is not removed");
        }
        const auto &ops = dict_ops();
        return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, storage_.data(), slot)};
    }

    bool TSDDataView::contains(const ValueView &key) const
    {
        const auto &ops = dict_ops();
        return ops.contains_impl(ops.context, storage_.data(), key);
    }

    std::size_t TSDDataView::find_slot(const ValueView &key) const
    {
        const auto &ops = dict_ops();
        return ops.find_slot_impl(ops.context, storage_.data(), key);
    }

    TSDataView TSDDataView::at_slot(std::size_t slot) const
    {
        if (!slot_occupied(slot)) { throw std::out_of_range("TSDDataView::at_slot: slot is not occupied"); }
        const auto &ops = dict_ops();
        const auto child_type = ops.child_binding_at_slot_impl != nullptr
                                    ? ops.child_binding_at_slot_impl(ops.context, storage_.data(), slot)
                                    : layout().element_type;
        const auto *child_memory = ops.child_at_slot_impl(ops.context, storage_.data(), slot);
        auto        parent       = base();
        if (!parent.ops().allows_mutation) { return TSDataView{child_type, child_memory}; }
        return TSDataView{child_type, child_memory, parent, slot};
    }

    TSDataView TSDDataView::at(const ValueView &key) const
    {
        const auto slot = find_slot(key);
        if (slot == TS_DATA_NO_CHILD_ID) { return TSDataView{layout().element_type, static_cast<const void *>(nullptr)}; }
        return at_slot(slot);
    }

    TSDataView TSDDataView::operator[](const ValueView &key) const
    {
        return at(key);
    }

    Range<ValueView> TSDDataView::keys() const
    {
        const auto &ops = dict_ops();
        return ops.make_values_range_impl(ops.context, storage_.data());
    }

    Range<TSDataView> TSDDataView::values() const
    {
        const auto &ops = dict_ops();
        return ops.make_ts_values_range_impl(ops.context, storage_.data());
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::items() const
    {
        const auto &ops = dict_ops();
        return ops.make_ts_kv_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSDDataView::valid_keys() const
    {
        const auto &ops = dict_ops();
        return ops.make_valid_keys_range_impl(ops.context, storage_.data());
    }

    Range<TSDataView> TSDDataView::valid_values() const
    {
        const auto &ops = dict_ops();
        return ops.make_valid_ts_values_range_impl(ops.context, storage_.data());
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::valid_items() const
    {
        const auto &ops = dict_ops();
        return ops.make_valid_ts_kv_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSDDataView::modified_keys(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_value_range(); }
        const auto &ops = dict_ops();
        return ops.make_modified_keys_range_impl(ops.context, storage_.data());
    }

    Range<TSDataView> TSDDataView::modified_values(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_ts_data_range(); }
        const auto &ops = dict_ops();
        return ops.make_modified_ts_values_range_impl(ops.context, storage_.data());
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::modified_items(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_ts_data_kv_range(); }
        const auto &ops = dict_ops();
        return ops.make_modified_ts_kv_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSDDataView::added_keys() const
    {
        return key_set().added();
    }

    Range<TSDataView> TSDDataView::added_values() const
    {
        const auto &ops = dict_ops();
        return ops.make_added_ts_values_range_impl(ops.context, storage_.data());
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::added_items() const
    {
        const auto &ops = dict_ops();
        return ops.make_added_ts_kv_range_impl(ops.context, storage_.data());
    }

    Range<ValueView> TSDDataView::removed_keys() const
    {
        return key_set().removed();
    }

    Range<TSDataView> TSDDataView::removed_values() const
    {
        const auto &ops = dict_ops();
        return ops.make_removed_ts_values_range_impl(ops.context, storage_.data());
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::removed_items() const
    {
        const auto &ops = dict_ops();
        return ops.make_removed_ts_kv_range_impl(ops.context, storage_.data());
    }

    TSSDataView TSDDataView::key_set() const
    {
        const auto key_set_type = layout().key_set_type;
        if (!key_set_type)
        {
            throw std::logic_error("TSDDataView::key_set requires a key-set binding");
        }
        return TSSDataView{TSDataView{key_set_type, storage_.data()}};
    }

    TSDDataMutationView TSDDataView::begin_mutation(DateTime evaluation_time) const
    {
        return TSDDataMutationView{base(), evaluation_time};
    }

    Range<ValueView> TSDDataView::empty_value_range() noexcept
    {
        return Range<ValueView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                .projector = nullptr};
    }

    Range<TSDataView> TSDDataView::empty_ts_data_range() noexcept
    {
        return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                 .projector = nullptr};
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::empty_ts_data_kv_range() noexcept
    {
        return KeyValueRange<ValueView, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                    .predicate = nullptr, .projector = nullptr};
    }

    const TSDDataOps &TSDDataView::dict_ops() const
    {
        return storage_.ops();
    }

    TSDDataMutationView::TSDDataMutationView(TSDataView view, DateTime evaluation_time)
        : TSDDataView(TSDataView{view.storage_ref()}),
          mutation_(view.begin_mutation(evaluation_time))
    {
        if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("TSDDataMutationView requires a TSD TSData kind");
        }
    }

    TSDDataMutationView::TSDDataMutationView(TSDDataMutationView &&) noexcept = default;

    TSDDataMutationView::~TSDDataMutationView() noexcept = default;

    TSDDataView TSDDataMutationView::view()
    {
        return TSDDataView{base()};
    }

    DateTime TSDDataMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    void TSDDataMutationView::reserve(std::size_t capacity)
    {
        const auto &ops = dict_ops();
        ops.reserve_impl(ops.context, mutation_.mutable_data(), capacity);
    }

    void TSDDataMutationView::touch()
    {
        const auto &ops = dict_ops();
        if (ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
        auto keys = key_set();
        if (keys.last_modified_time() == MIN_DT)
        {
            static_cast<void>(keys.base().mutable_tracking().record_modified(current_mutation_time()));
        }
    }

    TSDataView TSDDataMutationView::at(const ValueView &key)
    {
        const auto &ops    = dict_ops();
        const auto  result = ops.insert_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        return at_slot(result.slot);
    }

    TSDataView TSDDataMutationView::operator[](const ValueView &key)
    {
        return at(key);
    }

    void TSDDataMutationView::set(const ValueView &key, const ValueView &value)
    {
        auto child = at(key);
        auto child_mutation = child.begin_mutation(current_mutation_time());
        static_cast<void>(child_mutation.copy_value_from(value));
    }

    bool TSDDataMutationView::erase(const ValueView &key)
    {
        const auto &ops    = dict_ops();
        const auto  result = ops.remove_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        if (!result.changed && ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
        return result.changed;
    }

    void TSDDataMutationView::clear()
    {
        std::vector<Value> current_keys;
        for (const auto key : keys()) { current_keys.emplace_back(key); }
        const auto &ops           = dict_ops();
        const bool  newly_touched = ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time());
        for (const auto &key : current_keys) { static_cast<void>(erase(key.view())); }
        if (newly_touched) { mutation_.mark_modified(); }
    }

    bool TSDDataMutationView::copy_value_from(const ValueView &source)
    {
        if (!source.has_value())
        {
            throw std::invalid_argument("TSDDataMutationView::copy_value_from requires a live source");
        }
        if (source.schema() != layout().value_binding.schema())
        {
            throw std::invalid_argument("TSDDataMutationView::copy_value_from requires the map value schema");
        }

        auto       source_map    = source.as_map();
        const bool newly_touched = !modified(current_mutation_time());
        touch();
        for (const auto [key, value] : source_map.items()) { set(key, value); }

        std::vector<Value> removals;
        for (const auto key : keys())
        {
            if (!source_map.contains(key)) { removals.emplace_back(key); }
        }
        for (const auto &key : removals) { static_cast<void>(erase(key.view())); }

        return newly_touched;
    }

    bool TSDDataMutationView::move_value_from(Value &&source)
    {
        return move_value_from(source.view());
    }

    bool TSDDataMutationView::move_value_from(ValueView source)
    {
        if (!source.has_value())
        {
            throw std::invalid_argument("TSDDataMutationView::move_value_from requires a live source");
        }
        if (source.schema() != layout().value_binding.schema())
        {
            throw std::invalid_argument("TSDDataMutationView::move_value_from requires the map value schema");
        }
        if (!source.writable_payload())
        {
            throw std::invalid_argument("TSDDataMutationView::move_value_from requires writable source storage");
        }

        auto       source_map = source.as_map();
        const auto &child_ops = layout().element_type.ops_ref();
        if (child_ops.move_value_from_impl == &ts_data_detail::missing_move_value_from)
        {
            throw std::logic_error(
                "TSDDataMutationView::move_value_from cannot relocate non-movable child TSData; update nested "
                "time-series collections in place");
        }
        for (auto [key, value] : source_map.items())
        {
            if (!key.valid() || !value.valid())
            {
                throw std::invalid_argument("TSDDataMutationView::move_value_from requires live source entries");
            }
        }

        std::vector<std::size_t> removals;
        for (std::size_t slot = 0; slot < slot_capacity(); ++slot)
        {
            if (!slot_live(slot)) { continue; }
            auto key = key_at_slot(slot);
            if (!source_map.contains(key)) { removals.push_back(slot); }
        }

        const bool newly_touched = !modified(current_mutation_time());
        touch();

        const auto &ops = dict_ops();
        for (auto [key, value] : source_map.items())
        {
            const auto result = ops.insert_key_move_impl(
                ops.context,
                mutation_.mutable_data(),
                key,
                current_mutation_time());
            apply_slot_mutation_result(mutation_, result);

            auto child = at_slot(result.slot);
            auto child_mutation = child.begin_mutation(current_mutation_time());
            ValueView source_child{value.binding(), const_cast<void *>(value.data())};
            static_cast<void>(child_mutation.move_value_from(std::move(source_child)));
        }

        for (const auto slot : removals)
        {
            const auto result = ops.remove_slot_impl(
                ops.context,
                mutation_.mutable_data(),
                slot,
                current_mutation_time());
            apply_slot_mutation_result(mutation_, result);
        }

        return newly_touched;
    }

    TSDataView TSDDataMutationView::at_slot(std::size_t slot)
    {
        const auto &ops = dict_ops();
        if (!ops.slot_occupied_impl(ops.context, mutation_.mutable_data(), slot))
        {
            throw std::out_of_range("TSDDataMutationView::at_slot: slot is not occupied");
        }
        const auto child_type = ops.child_binding_at_slot_impl != nullptr
                                    ? ops.child_binding_at_slot_impl(ops.context, mutation_.mutable_data(), slot)
                                    : layout().element_type;
        const auto *child_memory = ops.child_at_slot_impl(ops.context, mutation_.mutable_data(), slot);
        return TSDataView{child_type, child_memory, mutation_.view(), slot};
    }
}  // namespace hgraph
