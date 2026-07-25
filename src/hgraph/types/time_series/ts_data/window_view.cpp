#include <hgraph/types/time_series/ts_data.h>

#include <stdexcept>

namespace hgraph
{
    TSWDataView::TSWDataView(TSDataView view)
        : storage_(view.storage_ref(), TSTypeKind::TSW)
    {
    }

    TSDataView TSWDataView::base() const noexcept
    {
        return TSDataView{storage_.storage_ref()};
    }

    const TSValueTypeMetaData *TSWDataView::schema() const noexcept
    {
        return base().schema();
    }

    const TSWDataLayout &TSWDataView::layout() const
    {
        return static_cast<const TSWDataLayout &>(base().layout());
    }

    const SizeTSWDataLayout &TSWDataView::size_layout() const
    {
        if (duration_based()) { throw std::logic_error("TSWDataView::size_layout requires a size-based TSW"); }
        return static_cast<const SizeTSWDataLayout &>(base().layout());
    }

    const TimeTSWDataLayout &TSWDataView::time_layout() const
    {
        if (!duration_based()) { throw std::logic_error("TSWDataView::time_layout requires a time-based TSW"); }
        return static_cast<const TimeTSWDataLayout &>(base().layout());
    }

    ValueView TSWDataView::value() const
    {
        return base().value();
    }

    ValueView TSWDataView::delta_value(DateTime evaluation_time) const
    {
        return base().delta_value(evaluation_time);
    }

    DateTime TSWDataView::last_modified_time() const
    {
        return base().last_modified_time();
    }

    bool TSWDataView::modified(DateTime evaluation_time) const
    {
        return base().modified(evaluation_time);
    }

    void TSWDataView::subscribe(Notifiable *observer) const
    {
        base().subscribe(observer);
    }

    void TSWDataView::unsubscribe(Notifiable *observer) const
    {
        base().unsubscribe(observer);
    }

    bool TSWDataView::has_observers() const
    {
        return base().has_observers();
    }

    std::size_t TSWDataView::observer_count() const
    {
        return base().observer_count();
    }

    bool TSWDataView::duration_based() const noexcept
    {
        return schema()->is_duration_based();
    }

    bool TSWDataView::size_based() const noexcept
    {
        return !duration_based();
    }

    bool TSWDataView::time_based() const noexcept
    {
        return duration_based();
    }

    std::size_t TSWDataView::period() const
    {
        return size_layout().period;
    }

    std::size_t TSWDataView::min_period() const
    {
        return size_layout().min_period;
    }

    TimeDelta TSWDataView::time_range() const
    {
        return time_layout().time_range;
    }

    TimeDelta TSWDataView::min_time_range() const
    {
        return time_layout().min_time_range;
    }

    std::size_t TSWDataView::capacity() const
    {
        const auto &ops = window_ops();
        return ops.capacity_impl(ops.context, storage_.data());
    }

    std::size_t TSWDataView::size() const
    {
        const auto &ops = window_ops();
        return ops.size_impl(ops.context, storage_.data());
    }

    bool TSWDataView::empty() const
    {
        return size() == 0;
    }

    bool TSWDataView::full() const
    {
        const auto &ops = window_ops();
        return ops.full_impl(ops.context, storage_.data());
    }

    bool TSWDataView::all_valid() const
    {
        return base().all_valid();
    }

    bool TSWDataView::has_removed_value(DateTime evaluation_time) const
    {
        const auto &ops = window_ops();
        if (ops.evicted_element_impl == nullptr || ops.evicted_time_impl == nullptr) { return false; }
        return evaluation_time != MIN_DT &&
               ops.evicted_time_impl(ops.context, storage_.data()) == evaluation_time &&
               ops.evicted_element_impl(ops.context, storage_.data()) != nullptr;
    }

    ValueView TSWDataView::removed_value(DateTime evaluation_time) const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        if (ops.evicted_element_impl == nullptr || ops.evicted_time_impl == nullptr || evaluation_time == MIN_DT ||
            ops.evicted_time_impl(ops.context, memory) != evaluation_time ||
            ops.evicted_element_impl(ops.context, memory) == nullptr)
        {
            throw std::logic_error("TSWDataView::removed_value: nothing was evicted this cycle");
        }
        const auto &data_layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{data_layout.element_binding, ops.evicted_element_impl(ops.context, memory)};
    }

    bool TSWDataView::cleared(DateTime evaluation_time) const
    {
        const auto &ops = window_ops();
        return evaluation_time != MIN_DT && ops.cleared_time_impl != nullptr &&
               ops.cleared_time_impl(ops.context, storage_.data()) == evaluation_time;
    }

    DateTime TSWDataView::first_modified_time() const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        return ops.size_impl(ops.context, memory) == 0 ? MIN_DT : ops.time_at_impl(ops.context, memory, 0);
    }

    DateTime TSWDataView::time_at(std::size_t index) const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        if (index >= ops.size_impl(ops.context, memory))
        {
            throw std::out_of_range("TSWDataView::time_at: index out of range");
        }
        return ops.time_at_impl(ops.context, memory, index);
    }

    ValueView TSWDataView::time_value_at(std::size_t index) const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        if (index >= ops.size_impl(ops.context, memory))
        {
            throw std::out_of_range("TSWDataView::time_value_at: index out of range");
        }
        const auto &data_layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{data_layout.time_binding, ops.time_element_at_impl(ops.context, memory, index)};
    }

    ValueView TSWDataView::at(std::size_t index) const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        if (index >= ops.size_impl(ops.context, memory))
        {
            throw std::out_of_range("TSWDataView::at: index out of range");
        }
        const auto &data_layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{data_layout.element_binding, ops.element_at_impl(ops.context, memory, index)};
    }

    ValueView TSWDataView::operator[](std::size_t index) const
    {
        return at(index);
    }

    ValueView TSWDataView::front() const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        if (ops.size_impl(ops.context, memory) == 0)
        {
            throw std::out_of_range("TSWDataView::front on empty window");
        }
        const auto &data_layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{data_layout.element_binding, ops.element_at_impl(ops.context, memory, 0)};
    }

    ValueView TSWDataView::back() const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        const auto window_size = ops.size_impl(ops.context, memory);
        if (window_size == 0) { throw std::out_of_range("TSWDataView::back on empty window"); }
        const auto &data_layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{data_layout.element_binding,
                         ops.element_at_impl(ops.context, memory, window_size - 1)};
    }

    Range<ValueView> TSWDataView::values() const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        return Range<ValueView>{
            .context   = &ops,
            .memory    = memory,
            .limit     = ops.size_impl(ops.context, memory),
            .predicate = nullptr,
            .projector = &project_value,
        };
    }

    Range<ValueView> TSWDataView::time_values() const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        return Range<ValueView>{
            .context   = &ops,
            .memory    = memory,
            .limit     = ops.size_impl(ops.context, memory),
            .predicate = nullptr,
            .projector = &project_time_value,
        };
    }

    Range<DateTime> TSWDataView::value_times() const
    {
        const auto &ops = window_ops();
        const auto *memory = storage_.data();
        return Range<DateTime>{
            .context   = &ops,
            .memory    = memory,
            .limit     = ops.size_impl(ops.context, memory),
            .predicate = nullptr,
            .projector = &project_time,
        };
    }

    Range<ValueView>::iterator TSWDataView::begin() const
    {
        return values().begin();
    }

    Range<ValueView>::iterator TSWDataView::end() const
    {
        return values().end();
    }

    TSWDataMutationView TSWDataView::begin_mutation(DateTime evaluation_time) const
    {
        return TSWDataMutationView{storage_, evaluation_time, TrustedStorageTag{}};
    }

    const TSWDataOps &TSWDataView::window_ops() const
    {
        return storage_.ops();
    }

    ValueView TSWDataView::project_value(const void *context, const void *memory, std::size_t index)
    {
        const auto &ops = *static_cast<const TSWDataOps *>(context);
        const auto &layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{layout.element_binding, ops.element_at_impl(ops.context, memory, index)};
    }

    ValueView TSWDataView::project_time_value(const void *context, const void *memory, std::size_t index)
    {
        const auto &ops = *static_cast<const TSWDataOps *>(context);
        const auto &layout = *static_cast<const TSWDataLayout *>(ops.layout_impl(ops.context));
        return ValueView{layout.time_binding, ops.time_element_at_impl(ops.context, memory, index)};
    }

    DateTime TSWDataView::project_time(const void *context, const void *memory, std::size_t index)
    {
        const auto &ops = *static_cast<const TSWDataOps *>(context);
        return ops.time_at_impl(ops.context, memory, index);
    }

    TSWDataMutationView::TSWDataMutationView(TSDataView view, DateTime evaluation_time)
        : TSWDataMutationView(TSWDataStorageRef{view.storage_ref(), TSTypeKind::TSW}, evaluation_time,
                              TrustedStorageTag{})
    {
    }

    TSWDataMutationView::TSWDataMutationView(TSWDataStorageRef storage, DateTime evaluation_time,
                                             TrustedStorageTag tag)
        : TSWDataView(storage, tag),
          mutation_(TSDataView{storage.storage_ref()}.begin_mutation(evaluation_time))
    {
    }

    TSWDataMutationView::TSWDataMutationView(TSWDataMutationView &&) noexcept = default;

    TSWDataMutationView::~TSWDataMutationView() noexcept = default;

    TSWDataView TSWDataMutationView::view()
    {
        return TSWDataView{base()};
    }

    DateTime TSWDataMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    void TSWDataMutationView::push(const ValueView &source)
    {
        const auto &ops = window_ops();
        if (mutation_.modified(ops, current_mutation_time()) && !cleared_)
        {
            throw std::logic_error("TSWDataMutationView::push allows only one window tick per evaluation time");
        }
        if (ops.push_impl == nullptr)
        {
            throw std::logic_error("TSWDataMutationView::push is not supported by this TSW ops");
        }
        ops.push_impl(ops.context, mutation_.mutable_data(ops), source, current_mutation_time());
        mutation_.mark_modified(ops);
        cleared_ = false;
    }

    void TSWDataMutationView::clear()
    {
        const auto &ops = window_ops();
        if (mutation_.modified(ops, current_mutation_time()))
        {
            throw std::logic_error("TSWDataMutationView::clear allows only one reset per evaluation time");
        }
        if (ops.clear_impl == nullptr)
        {
            throw std::logic_error("TSWDataMutationView::clear is not supported by this TSW ops");
        }
        ops.clear_impl(ops.context, mutation_.mutable_data(ops), current_mutation_time());
        mutation_.mark_modified(ops);
        cleared_ = true;
    }

    bool TSWDataMutationView::copy_value_from(const ValueView &source)
    {
        if (cleared(current_mutation_time()))
        {
            throw std::logic_error(
                "TSWDataMutationView::copy_value_from cannot replace a window after clear in the same evaluation time");
        }
        return mutation_.copy_value_from(source);
    }
}  // namespace hgraph
