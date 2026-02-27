#include "ts_ops_internal.h"

namespace hgraph {

const engine_time_t* op_window_value_times(const ViewData& vd) {
    static thread_local std::vector<engine_time_t> cached_times;

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return nullptr;
    }
    const TSMeta* current = meta_at_path(resolved.meta, resolved.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return nullptr;
    }

    auto* time_root = static_cast<const Value*>(resolved.time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return nullptr;
    }

    auto time_path = ts_path_to_time_path(resolved.meta, resolved.path.indices);
    if (time_path.empty()) {
        return nullptr;
    }
    time_path.pop_back();

    std::optional<View> maybe_time_tuple;
    if (time_path.empty()) {
        maybe_time_tuple = time_root->view();
    } else {
        maybe_time_tuple = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time_tuple.has_value() || !maybe_time_tuple->valid() || !maybe_time_tuple->is_tuple()) {
        return nullptr;
    }

    auto tuple = maybe_time_tuple->as_tuple();
    if (tuple.size() < 2) {
        return nullptr;
    }

    cached_times.clear();
    if (current->is_duration_based()) {
        View time_values = tuple.at(1);
        if (!time_values.valid() || !time_values.is_queue()) {
            return nullptr;
        }
        auto queue = time_values.as_queue();
        cached_times.reserve(queue.size());
        for (size_t i = 0; i < queue.size(); ++i) {
            const auto* t = static_cast<const engine_time_t*>(
                value::QueueOps::get_element_ptr_const(queue.data(), i, queue.schema()));
            if (t != nullptr) {
                cached_times.push_back(*t);
            }
        }
    } else {
        View time_values = tuple.at(1);
        if (!time_values.valid() || !time_values.is_cyclic_buffer()) {
            return nullptr;
        }
        auto buffer = time_values.as_cyclic_buffer();
        if (buffer.size() < current->min_period()) {
            return nullptr;
        }
        cached_times.reserve(buffer.size());
        for (size_t i = 0; i < buffer.size(); ++i) {
            const auto* t = static_cast<const engine_time_t*>(
                value::CyclicBufferOps::get_element_ptr_const(buffer.data(), i, buffer.schema()));
            if (t != nullptr) {
                cached_times.push_back(*t);
            }
        }
    }

    return cached_times.empty() ? nullptr : cached_times.data();
}

size_t op_window_value_times_count(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return 0;
    }
    const TSMeta* current = meta_at_path(resolved.meta, resolved.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return 0;
    }

    auto* time_root = static_cast<const Value*>(resolved.time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return 0;
    }

    auto time_path = ts_path_to_time_path(resolved.meta, resolved.path.indices);
    if (time_path.empty()) {
        return 0;
    }
    time_path.pop_back();

    std::optional<View> maybe_time_tuple;
    if (time_path.empty()) {
        maybe_time_tuple = time_root->view();
    } else {
        maybe_time_tuple = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time_tuple.has_value() || !maybe_time_tuple->valid() || !maybe_time_tuple->is_tuple()) {
        return 0;
    }

    auto tuple = maybe_time_tuple->as_tuple();
    if (tuple.size() < 2) {
        return 0;
    }

    if (current->is_duration_based()) {
        View time_values = tuple.at(1);
        return (time_values.valid() && time_values.is_queue()) ? time_values.as_queue().size() : 0;
    }

    View time_values = tuple.at(1);
    if (!time_values.valid() || !time_values.is_cyclic_buffer()) {
        return 0;
    }
    const size_t count = time_values.as_cyclic_buffer().size();
    return count >= current->min_period() ? count : 0;
}

engine_time_t op_window_first_modified_time(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return MIN_DT;
    }
    const TSMeta* current = meta_at_path(resolved.meta, resolved.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return MIN_DT;
    }

    auto* time_root = static_cast<const Value*>(resolved.time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return MIN_DT;
    }

    auto time_path = ts_path_to_time_path(resolved.meta, resolved.path.indices);
    if (time_path.empty()) {
        return MIN_DT;
    }
    time_path.pop_back();

    std::optional<View> maybe_time_tuple;
    if (time_path.empty()) {
        maybe_time_tuple = time_root->view();
    } else {
        maybe_time_tuple = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time_tuple.has_value() || !maybe_time_tuple->valid() || !maybe_time_tuple->is_tuple()) {
        return MIN_DT;
    }

    auto tuple = maybe_time_tuple->as_tuple();
    if (tuple.size() < 2) {
        return MIN_DT;
    }

    View time_values = tuple.at(1);
    if (current->is_duration_based()) {
        if (!time_values.valid() || !time_values.is_queue() || time_values.as_queue().size() == 0) {
            return MIN_DT;
        }
        auto queue = time_values.as_queue();
        const auto* first = static_cast<const engine_time_t*>(
            value::QueueOps::get_element_ptr_const(queue.data(), 0, queue.schema()));
        return first != nullptr ? *first : MIN_DT;
    }

    if (!time_values.valid() || !time_values.is_cyclic_buffer() || time_values.as_cyclic_buffer().size() == 0) {
        return MIN_DT;
    }
    auto buffer = time_values.as_cyclic_buffer();
    const auto* first = static_cast<const engine_time_t*>(
        value::CyclicBufferOps::get_element_ptr_const(buffer.data(), 0, buffer.schema()));
    return first != nullptr ? *first : MIN_DT;
}

bool op_window_has_removed_value(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return false;
    }
    const TSMeta* current = meta_at_path(resolved.meta, resolved.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return false;
    }

    if (current->is_duration_based()) {
        auto slots = resolve_tsw_duration_delta_slots(resolved);
        return slots.has_removed.valid() &&
               slots.has_removed.is_scalar_type<bool>() &&
               slots.has_removed.as<bool>();
    }

    auto slots = resolve_tsw_tick_delta_slots(resolved);
    return slots.has_removed.valid() &&
           slots.has_removed.is_scalar_type<bool>() &&
           slots.has_removed.as<bool>();
}

View op_window_removed_value(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return {};
    }
    const TSMeta* current = meta_at_path(resolved.meta, resolved.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return {};
    }

    if (current->is_duration_based()) {
        auto slots = resolve_tsw_duration_delta_slots(resolved);
        if (!(slots.has_removed.valid() &&
              slots.has_removed.is_scalar_type<bool>() &&
              slots.has_removed.as<bool>())) {
            return {};
        }
        if (slots.removed_values.valid() &&
            slots.removed_values.is_queue() &&
            slots.removed_values.as_queue().size() > 0) {
            return slots.removed_values;
        }
        return {};
    }

    auto slots = resolve_tsw_tick_delta_slots(resolved);
    if (!(slots.has_removed.valid() &&
          slots.has_removed.is_scalar_type<bool>() &&
          slots.has_removed.as<bool>())) {
        return {};
    }
    return slots.removed_value.valid() ? slots.removed_value : View{};
}

size_t op_window_removed_value_count(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return 0;
    }
    const TSMeta* current = meta_at_path(resolved.meta, resolved.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return 0;
    }

    if (current->is_duration_based()) {
        auto slots = resolve_tsw_duration_delta_slots(resolved);
        if (!(slots.has_removed.valid() &&
              slots.has_removed.is_scalar_type<bool>() &&
              slots.has_removed.as<bool>())) {
            return 0;
        }
        return (slots.removed_values.valid() && slots.removed_values.is_queue())
                   ? slots.removed_values.as_queue().size()
                   : 0;
    }

    auto slots = resolve_tsw_tick_delta_slots(resolved);
    if (!(slots.has_removed.valid() &&
          slots.has_removed.is_scalar_type<bool>() &&
          slots.has_removed.as<bool>())) {
        return 0;
    }
    return slots.removed_value.valid() ? size_t{1} : size_t{0};
}

size_t op_window_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return 0;
    }
    return current->is_duration_based()
               ? static_cast<size_t>(std::max<int64_t>(0, current->time_range().count()))
               : current->period();
}

size_t op_window_min_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (!dispatch_meta_is_tsw(current)) {
        return 0;
    }
    return current->is_duration_based()
               ? static_cast<size_t>(std::max<int64_t>(0, current->min_time_range().count()))
               : current->min_period();
}

size_t op_window_length(const ViewData& vd) {
    View v = op_value(vd);
    if (!v.valid()) {
        return 0;
    }
    if (v.is_cyclic_buffer()) {
        return v.as_cyclic_buffer().size();
    }
    if (v.is_queue()) {
        return v.as_queue().size();
    }
    return 0;
}


const ts_window_ops k_window_ops{
    &op_window_value_times,
    &op_window_value_times_count,
    &op_window_first_modified_time,
    &op_window_has_removed_value,
    &op_window_removed_value,
    &op_window_removed_value_count,
    &op_window_size,
    &op_window_min_size,
    &op_window_length,
};

}  // namespace hgraph
