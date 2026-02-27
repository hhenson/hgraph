#include "ts_ops_internal.h"

namespace hgraph {

void op_from_python_tsw(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || src.is_none()) {
        return;
    }

    auto maybe_window = resolve_value_slot_mut(vd);
    if (!maybe_window.has_value()) {
        return;
    }

    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return;
    }
    if (!time_root->has_value()) {
        time_root->emplace();
    }

    auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    if (time_path.empty()) {
        return;
    }
    time_path.pop_back();

    std::optional<ValueView> maybe_time_tuple;
    if (time_path.empty()) {
        maybe_time_tuple = time_root->view();
    } else {
        maybe_time_tuple = navigate_mut(time_root->view(), time_path);
    }
    if (!maybe_time_tuple.has_value() || !maybe_time_tuple->valid() || !maybe_time_tuple->is_tuple()) {
        return;
    }

    auto time_tuple = maybe_time_tuple->as_tuple();
    if (time_tuple.size() < 2) {
        return;
    }
    ValueView container_time = time_tuple.at(0);
    if (!container_time.valid() || !container_time.is_scalar_type<engine_time_t>()) {
        return;
    }

    auto maybe_value = value_from_python(current->value_type, src);
    if (!maybe_value.has_value()) {
        return;
    }

    clear_tsw_delta_if_new_tick(vd, current_time);

    const ts_ops* current_ops = vd.ops != nullptr ? vd.ops : dispatch_meta_ops(current);
    if (dispatch_ops_is_tsw_duration(current_ops)) {
        if (!maybe_window->valid() || !maybe_window->is_queue()) {
            return;
        }
        ValueView time_values = time_tuple.at(1);
        if (!time_values.valid() || !time_values.is_queue()) {
            return;
        }

        auto window_values = maybe_window->as_queue();
        auto window_times = time_values.as_queue();

        ValueView start_time = time_tuple.size() > 2 ? time_tuple.at(2) : ValueView{};
        ValueView ready = time_tuple.size() > 3 ? time_tuple.at(3) : ValueView{};
        if (start_time.valid() && start_time.is_scalar_type<engine_time_t>() &&
            start_time.as<engine_time_t>() <= MIN_DT) {
            start_time.as<engine_time_t>() = current_time;
        }
        if (ready.valid() && ready.is_scalar_type<bool>() &&
            start_time.valid() && start_time.is_scalar_type<engine_time_t>() &&
            !ready.as<bool>()) {
            ready.as<bool>() = (current_time - start_time.as<engine_time_t>()) >= current->min_time_range();
        }

        value::QueueOps::push(window_values.data(), maybe_value->data(), window_values.schema());
        value::QueueOps::push(window_times.data(), &current_time, window_times.schema());

        TSWDurationDeltaSlots delta_slots = resolve_tsw_duration_delta_slots(vd);
        auto append_removed = [&](const void* removed_value_ptr) {
            if (removed_value_ptr == nullptr) {
                return;
            }
            if (delta_slots.removed_values.valid() && delta_slots.removed_values.is_queue()) {
                auto removed_values = delta_slots.removed_values.as_queue();
                value::QueueOps::push(removed_values.data(), removed_value_ptr, removed_values.schema());
            }
            if (delta_slots.has_removed.valid() && delta_slots.has_removed.is_scalar_type<bool>()) {
                delta_slots.has_removed.as<bool>() = true;
            }
        };

        const engine_time_t cutoff = current_time - current->time_range();
        while (window_times.size() > 0) {
            const auto* oldest_time = static_cast<const engine_time_t*>(
                value::QueueOps::get_element_ptr_const(window_times.data(), 0, window_times.schema()));
            if (oldest_time == nullptr || *oldest_time >= cutoff) {
                break;
            }

            const void* oldest_value =
                value::QueueOps::get_element_ptr_const(window_values.data(), 0, window_values.schema());
            append_removed(oldest_value);
            value::QueueOps::pop(window_values.data(), window_values.schema());
            value::QueueOps::pop(window_times.data(), window_times.schema());
        }
    } else {
        if (!maybe_window->valid() || !maybe_window->is_cyclic_buffer()) {
            return;
        }
        ValueView time_values = time_tuple.at(1);
        if (!time_values.valid() || !time_values.is_cyclic_buffer()) {
            return;
        }

        auto window_values = maybe_window->as_cyclic_buffer();
        auto window_times = time_values.as_cyclic_buffer();

        if (window_values.size() == window_values.capacity()) {
            TSWTickDeltaSlots delta_slots = resolve_tsw_tick_delta_slots(vd);
            const void* oldest_value = value::CyclicBufferOps::get_element_ptr_const(
                window_values.data(), 0, window_values.schema());
            if (oldest_value != nullptr &&
                delta_slots.removed_value.valid() &&
                current->value_type != nullptr &&
                delta_slots.removed_value.schema() == current->value_type) {
                current->value_type->ops().copy(delta_slots.removed_value.data(), oldest_value, current->value_type);
            }
            if (delta_slots.has_removed.valid() && delta_slots.has_removed.is_scalar_type<bool>()) {
                delta_slots.has_removed.as<bool>() = true;
            }
        }

        value::CyclicBufferOps::push(window_values.data(), maybe_value->data(), window_values.schema());
        value::CyclicBufferOps::push(window_times.data(), &current_time, window_times.schema());
    }

    container_time.as<engine_time_t>() = current_time;
    stamp_time_paths(vd, current_time);
    mark_tsd_parent_child_modified(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

}  // namespace hgraph
