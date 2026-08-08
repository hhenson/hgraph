#include <hgraph/types/time_series/ts_data.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
    TSDataView IndexedTSDataView::base() const noexcept
    {
        return TSDataView{storage_.storage_ref()};
    }

    const TSValueTypeMetaData *IndexedTSDataView::schema() const noexcept
    {
        return base().schema();
    }

    const TSDataLayout &IndexedTSDataView::layout() const
    {
        return base().layout();
    }

    ValueView IndexedTSDataView::value() const
    {
        return base().value();
    }

    ValueView IndexedTSDataView::delta_value(DateTime evaluation_time) const
    {
        return base().delta_value(evaluation_time);
    }

    DateTime IndexedTSDataView::last_modified_time() const
    {
        return base().last_modified_time();
    }

    bool IndexedTSDataView::modified(DateTime evaluation_time) const
    {
        return base().modified(evaluation_time);
    }

    void IndexedTSDataView::subscribe(Notifiable *observer) const
    {
        base().subscribe(observer);
    }

    void IndexedTSDataView::unsubscribe(Notifiable *observer) const
    {
        base().unsubscribe(observer);
    }

    bool IndexedTSDataView::has_observers() const
    {
        return base().has_observers();
    }

    std::size_t IndexedTSDataView::observer_count() const
    {
        return base().observer_count();
    }

    std::size_t IndexedTSDataView::size() const
    {
        const auto &ops = indexed_ops();
        return ops.size_impl(ops.context, storage_.data());
    }

    bool IndexedTSDataView::empty() const
    {
        return size() == 0;
    }

    TSDataView IndexedTSDataView::at(std::size_t index) &
    {
        return at_impl(index);
    }

    TSDataView IndexedTSDataView::at(std::size_t index) const &
    {
        return const_cast<IndexedTSDataView *>(this)->at_impl(index);
    }

    TSDataView IndexedTSDataView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSDataView IndexedTSDataView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    Range<TSDataView> IndexedTSDataView::values() const
    {
        return values_range(nullptr);
    }

    Range<TSDataView> IndexedTSDataView::valid_values() const
    {
        return values_range(&child_valid_predicate);
    }

    Range<TSDataView> IndexedTSDataView::modified_values(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_values_range(); }
        return values_range(&child_modified_predicate);
    }

    Range<std::size_t> IndexedTSDataView::modified_indices() const
    {
        const auto &ops = indexed_ops();
        if (ops.modified_index_count_impl != nullptr && ops.modified_index_at_impl != nullptr)
        {
            return Range<std::size_t>{
                .context = this,
                .memory = nullptr,
                .limit = ops.modified_index_count_impl(ops.context, storage_.data()),
                .predicate = nullptr,
                .projector = &project_modified_index,
            };
        }
        return Range<std::size_t>{
            .context = this,
            .memory = nullptr,
            .limit = size(),
            .predicate = &child_modified_predicate,
            .projector = &project_modified_index,
        };
    }

    KeyValueRange<std::size_t, TSDataView> IndexedTSDataView::items() const
    {
        return items_range(nullptr);
    }

    KeyValueRange<std::size_t, TSDataView> IndexedTSDataView::valid_items() const
    {
        return items_range(&child_valid_predicate);
    }

    KeyValueRange<std::size_t, TSDataView> IndexedTSDataView::modified_items(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_items_range(); }
        return items_range(&child_modified_predicate);
    }

    IndexedTSDataView::IndexedTSDataView(TSDataView view, TSTypeKind expected_kind)
        : storage_(view.storage_ref(), expected_kind)
    {
    }

    bool IndexedTSDataView::child_valid(std::size_t index) const
    {
        return child_last_modified_time(index) != MIN_DT;
    }

    bool IndexedTSDataView::child_modified_at_parent_time(std::size_t index) const
    {
        return child_last_modified_time(index) == last_modified_time();
    }

    const IndexedTSDataOps &IndexedTSDataView::indexed_ops() const
    {
        return storage_.ops();
    }

    Range<TSDataView> IndexedTSDataView::values_range(Range<TSDataView>::predicate_fn predicate) const
    {
        return Range<TSDataView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = predicate,
            .projector = &project_value,
        };
    }

    KeyValueRange<std::size_t, TSDataView> IndexedTSDataView::items_range(
        KeyValueRange<std::size_t, TSDataView>::predicate_fn predicate) const
    {
        return KeyValueRange<std::size_t, TSDataView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = predicate,
            .projector = &project_item,
        };
    }

    DateTime IndexedTSDataView::child_last_modified_time(std::size_t index) const
    {
        const auto &ops = indexed_ops();
        if (index >= ops.size_impl(ops.context, storage_.data())) { return MIN_DT; }
        const auto element_type = ops.element_binding_impl(ops.context, storage_.data(), index);
        const auto *element_memory  = ops.element_memory_impl(ops.context, storage_.data(), index);
        if (!element_type || element_memory == nullptr) { return MIN_DT; }
        const auto &element_ops = *element_type.ops();
        return element_ops.tracking_impl(element_ops.context, element_memory)->last_modified_time;
    }

    TSDataView IndexedTSDataView::at_impl(std::size_t index)
    {
        const auto &ops = indexed_ops();
        if (index >= ops.size_impl(ops.context, storage_.data()))
        {
            throw std::out_of_range("IndexedTSDataView::at: index out of range");
        }
        const auto element_type = ops.element_binding_impl(ops.context, storage_.data(), index);
        if (!element_type)
        {
            throw std::logic_error("IndexedTSDataView::at: element binding is not resolved");
        }
        const auto *element_memory = ops.element_memory_impl(ops.context, storage_.data(), index);
        if (element_memory == nullptr)
        {
            return TSDataView{element_type, element_memory};
        }
        auto parent = base();
        if (!parent.ops().allows_mutation) { return TSDataView{element_type, element_memory}; }
        return TSDataView{element_type, element_memory, parent, index};
    }

    Range<TSDataView> IndexedTSDataView::empty_values_range() noexcept
    {
        return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                 .projector = nullptr};
    }

    KeyValueRange<std::size_t, TSDataView> IndexedTSDataView::empty_items_range() noexcept
    {
        return KeyValueRange<std::size_t, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                      .predicate = nullptr, .projector = nullptr};
    }

    bool IndexedTSDataView::child_valid_predicate(const void *context, const void *, std::size_t index)
    {
        return static_cast<const IndexedTSDataView *>(context)->child_valid(index);
    }

    bool IndexedTSDataView::child_modified_predicate(const void *context, const void *, std::size_t index)
    {
        return static_cast<const IndexedTSDataView *>(context)->child_modified_at_parent_time(index);
    }

    std::size_t IndexedTSDataView::project_modified_index(const void *context, const void *,
                                                          std::size_t ordinal)
    {
        const auto *self = static_cast<const IndexedTSDataView *>(context);
        const auto &ops = self->indexed_ops();
        return ops.modified_index_at_impl != nullptr
                   ? ops.modified_index_at_impl(ops.context, self->storage_.data(), ordinal)
                   : ordinal;
    }

    TSDataView IndexedTSDataView::project_value(const void *context, const void *, std::size_t index)
    {
        return const_cast<IndexedTSDataView *>(static_cast<const IndexedTSDataView *>(context))->at(index);
    }

    std::pair<std::size_t, TSDataView> IndexedTSDataView::project_item(const void *context, const void *,
                                                                       std::size_t index)
    {
        return {index, project_value(context, nullptr, index)};
    }

    TSBDataView::TSBDataView(TSDataView view)
        : IndexedTSDataView(std::move(view), TSTypeKind::TSB)
    {
    }

    TSDataView TSBDataView::at(std::string_view name) &
    {
        return IndexedTSDataView::at(field_index(name));
    }

    TSDataView TSBDataView::at(std::string_view name) const &
    {
        return const_cast<TSBDataView *>(this)->at(name);
    }

    TSDataView TSBDataView::field(std::string_view name) &
    {
        return at(name);
    }

    TSDataView TSBDataView::field(std::string_view name) const &
    {
        return at(name);
    }

    TSDataView TSBDataView::operator[](std::string_view name) &
    {
        return at(name);
    }

    TSDataView TSBDataView::operator[](std::string_view name) const &
    {
        return at(name);
    }

    bool TSBDataView::has_field(std::string_view name) const noexcept
    {
        return find_field_index(name) != npos;
    }

    Range<std::string_view> TSBDataView::keys() const
    {
        return Range<std::string_view>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = nullptr,
            .projector = &project_key,
        };
    }

    KeyValueRange<std::string_view, TSDataView> TSBDataView::items() const
    {
        return named_items_range(nullptr);
    }

    KeyValueRange<std::string_view, TSDataView> TSBDataView::valid_items() const
    {
        return named_items_range(&named_child_valid_predicate);
    }

    KeyValueRange<std::string_view, TSDataView> TSBDataView::modified_items(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_named_items_range(); }
        return named_items_range(&named_child_modified_predicate);
    }

    std::size_t TSBDataView::field_index(std::string_view name) const
    {
        const auto index = find_field_index(name);
        if (index == npos) { throw std::out_of_range("TSBDataView::at: field not found"); }
        return index;
    }

    std::size_t TSBDataView::find_field_index(std::string_view name) const noexcept
    {
        const auto *meta = schema();
        if (meta == nullptr || meta->kind != TSTypeKind::TSB) { return npos; }
        for (std::size_t index = 0; index < meta->field_count(); ++index)
        {
            const char *field_name = meta->fields()[index].name;
            if (field_name != nullptr && name == field_name) { return index; }
        }
        return npos;
    }

    std::string_view TSBDataView::key_at(std::size_t index) const noexcept
    {
        const auto *meta = schema();
        if (meta == nullptr || meta->kind != TSTypeKind::TSB || index >= meta->field_count()) { return {}; }
        const char *field_name = meta->fields()[index].name;
        return field_name != nullptr ? std::string_view{field_name} : std::string_view{};
    }

    KeyValueRange<std::string_view, TSDataView> TSBDataView::named_items_range(
        KeyValueRange<std::string_view, TSDataView>::predicate_fn predicate) const
    {
        return KeyValueRange<std::string_view, TSDataView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = predicate,
            .projector = &project_named_item,
        };
    }

    KeyValueRange<std::string_view, TSDataView> TSBDataView::empty_named_items_range() noexcept
    {
        return KeyValueRange<std::string_view, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                           .predicate = nullptr, .projector = nullptr};
    }

    bool TSBDataView::named_child_valid_predicate(const void *context, const void *, std::size_t index)
    {
        return static_cast<const TSBDataView *>(context)->child_valid(index);
    }

    bool TSBDataView::named_child_modified_predicate(const void *context, const void *, std::size_t index)
    {
        return static_cast<const TSBDataView *>(context)->child_modified_at_parent_time(index);
    }

    std::string_view TSBDataView::project_key(const void *context, const void *, std::size_t index)
    {
        return static_cast<const TSBDataView *>(context)->key_at(index);
    }

    std::pair<std::string_view, TSDataView> TSBDataView::project_named_item(const void *context, const void *,
                                                                            std::size_t index)
    {
        auto *self = const_cast<TSBDataView *>(static_cast<const TSBDataView *>(context));
        return {self->key_at(index), self->IndexedTSDataView::at(index)};
    }

    TSLDataView::TSLDataView(TSDataView view)
        : IndexedTSDataView(std::move(view), TSTypeKind::TSL)
    {
    }
}  // namespace hgraph
