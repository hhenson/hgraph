#include "ts_ops_internal.h"

namespace hgraph {

void apply_fallback_from_python_write(ViewData& vd, const nb::object& src, engine_time_t current_time);
void notify_if_static_container_children_changed(bool changed, const ViewData& vd, engine_time_t current_time);
void record_unbound_ref_item_changes(const ViewData& source,
                                     const std::vector<size_t>& changed_indices,
                                     engine_time_t current_time);

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    const bool debug_ref_from = std::getenv("HGRAPH_DEBUG_REF_FROM") != nullptr;
    const ts_ops* current_ops = dispatch_meta_ops(current);

    if (current_ops != nullptr &&
        current_ops->from_python != nullptr &&
        current_ops->from_python != &op_from_python) {
        current_ops->from_python(vd, src, current_time);
        return;
    }

    if (dispatch_meta_is_ref(current)) {
        // Python TimeSeriesReferenceOutput.apply_result(None) is a no-op.
        if (src.is_none()) {
            return;
        }

        auto maybe_dst = resolve_value_slot_mut(vd);
        if (!maybe_dst.has_value()) {
            return;
        }

        nb::object normalized_src = src;
        bool same_ref_identity = false;
        TimeSeriesReference existing_ref = TimeSeriesReference::make();
        bool existing_ref_valid = false;
        TimeSeriesReference incoming_ref = TimeSeriesReference::make();
        bool incoming_ref_valid = false;

        if (maybe_dst->valid() && maybe_dst->schema() == ts_reference_meta()) {
            existing_ref = nb::cast<TimeSeriesReference>(maybe_dst->to_python());
            existing_ref_valid = true;
            incoming_ref = nb::cast<TimeSeriesReference>(normalized_src);
            incoming_ref_valid = true;
            same_ref_identity = (existing_ref == incoming_ref);
        }

        const bool existing_payload_valid = existing_ref_valid && existing_ref.is_valid();
        const bool incoming_payload_valid = incoming_ref_valid && incoming_ref.is_valid();
        const bool has_prior_write = direct_last_modified_time(vd) != MIN_DT;
        const bool suppress_invalid_rebind_tick =
            vd.uses_link_target &&
            incoming_ref_valid &&
            existing_ref_valid &&
            !incoming_payload_valid &&
            !existing_payload_valid;

        if (same_ref_identity) {
            if (debug_ref_from) {
                std::fprintf(stderr,
                             "[ref_from_same] path=%s now=%lld existing_ref_valid=%d existing_payload_valid=%d incoming_ref_valid=%d incoming_payload_valid=%d has_prior_write=%d suppress=%d\n",
                             vd.path.to_string().c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()),
                             existing_ref_valid ? 1 : 0,
                             existing_payload_valid ? 1 : 0,
                             incoming_ref_valid ? 1 : 0,
                             incoming_payload_valid ? 1 : 0,
                             has_prior_write ? 1 : 0,
                             suppress_invalid_rebind_tick ? 1 : 0);
            }
            if (suppress_invalid_rebind_tick) {
                return;
            }
            const TSMeta* element_meta = current->element_ts();
            const bool dynamic_ref_container =
                element_meta != nullptr &&
                (dispatch_meta_is_tss(element_meta) || dispatch_meta_is_tsd(element_meta));
            if (dynamic_ref_container) {
                bool bound_target_modified = false;
                if (incoming_ref_valid && incoming_payload_valid) {
                    if (const ViewData* bound = incoming_ref.bound_view(); bound != nullptr) {
                        ViewData bound_view = *bound;
                        bound_view.sampled = bound_view.sampled || vd.sampled;
                        if (bound_view.ops != nullptr && bound_view.ops->modified != nullptr) {
                            bound_target_modified = bound_view.ops->modified(bound_view, current_time);
                        }
                    }
                }
                if (!bound_target_modified) {
                    return;
                }
            }
            stamp_time_paths(vd, current_time);
            mark_tsd_parent_child_modified(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        if (debug_ref_from) {
            std::string in_repr{"<repr_error>"};
            try {
                in_repr = nb::cast<std::string>(nb::repr(normalized_src));
            } catch (...) {}
            std::fprintf(stderr,
                         "[ref_from] path=%s now=%lld before_valid=%d same=%d existing_payload_valid=%d incoming_payload_valid=%d suppress=%d in=%s\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         maybe_dst->valid() ? 1 : 0,
                         same_ref_identity ? 1 : 0,
                         existing_payload_valid ? 1 : 0,
                         incoming_payload_valid ? 1 : 0,
                         suppress_invalid_rebind_tick ? 1 : 0,
                         in_repr.c_str());
        }

        maybe_dst->from_python(normalized_src);

        TimeSeriesReference written_ref = TimeSeriesReference::make();
        bool written_ref_valid = false;
        if (maybe_dst->valid() && maybe_dst->schema() == ts_reference_meta()) {
            written_ref = incoming_ref_valid ? incoming_ref : nb::cast<TimeSeriesReference>(maybe_dst->to_python());
            written_ref_valid = true;
        }

        if (auto* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
            unregister_ref_link_observer(*ref_link);

            bool has_bound_target = false;
            if (written_ref_valid) {
                if (const ViewData* bound = written_ref.bound_view(); bound != nullptr) {
                    store_to_ref_link(*ref_link, *bound);
                    has_bound_target = true;
                }
            }

            if (!has_bound_target) {
                ref_link->unbind();
            }

            if (current_time != MIN_DT && !suppress_invalid_rebind_tick) {
                ref_link->last_rebind_time = current_time;
            }

            if (has_bound_target) {
                register_ref_link_observer(*ref_link, &vd);
            }
        }

        if (debug_ref_from && maybe_dst->valid()) {
            std::string out_repr{"<repr_error>"};
            try {
                out_repr = nb::cast<std::string>(nb::repr(maybe_dst->to_python()));
            } catch (...) {}
            std::fprintf(stderr,
                         "[ref_from] path=%s now=%lld after=%s\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         out_repr.c_str());
        }

        if (suppress_invalid_rebind_tick) {
            // Keep REF wrapper invalid-time semantics (no modified/lmt tick)
            // but still notify active linked inputs that the wrapper payload
            // changed shape on first invalid materialization.
            if (current_time != MIN_DT && !has_prior_write) {
                notify_link_target_observers(vd, current_time);
            }
            return;
        }

        // When writing REF[TSB]/REF[TSL] composites, only stamp child timestamps
        // for item-level reference identity changes. This preserves per-field delta
        // behavior when only a subset of item bindings re-point.
        if (current_time != MIN_DT && written_ref_valid && written_ref.is_unbound()) {
            const auto& new_items = written_ref.items();
            std::vector<size_t> changed_indices;
            bool can_compare_items =
                existing_ref_valid &&
                existing_ref.is_unbound() &&
                existing_ref.items().size() == new_items.size();

            for (size_t i = 0; i < new_items.size(); ++i) {
                bool changed_item = true;
                if (can_compare_items) {
                    changed_item = !(existing_ref.items()[i] == new_items[i]);
                }
                if (debug_ref_from) {
                    std::fprintf(stderr,
                                 "[ref_from_items] path=%s now=%lld idx=%zu changed=%d can_compare=%d\n",
                                 vd.path.to_string().c_str(),
                                 static_cast<long long>(current_time.time_since_epoch().count()),
                                 i,
                                 changed_item ? 1 : 0,
                                 can_compare_items ? 1 : 0);
                }
                if (!changed_item) {
                    continue;
                }
                changed_indices.push_back(i);
                ViewData child = vd;
                child.path.indices.push_back(i);
                stamp_time_paths(child, current_time);
            }

            record_unbound_ref_item_changes(vd, changed_indices, current_time);
        }

        stamp_time_paths(vd, current_time);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    if (dispatch_meta_is_tsw(current)) {
        if (src.is_none()) {
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
        return;
    }

    if (dispatch_meta_is_tss(current)) {
        if (reset_root_value_and_delta_on_none(vd, src, current_time)) {
            return;
        }

        auto maybe_set = resolve_value_slot_mut(vd);
        if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
            return;
        }

        const bool was_valid = op_valid(vd);
        auto slots = resolve_tss_delta_slots(vd);
        clear_tss_delta_if_new_tick(vd, current_time, slots);

        auto set = maybe_set->as_set();
        const value::TypeMeta* element_type = set.element_type();
        if (element_type == nullptr) {
            return;
        }

        auto apply_add = [&](const View& elem) -> bool {
            if (!elem.valid()) {
                return false;
            }
            // Python parity for SetDelta inputs: when an element is marked
            // removed in this tick, do not re-add it via the added set.
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                auto removed = slots.removed_set.as_set();
                if (removed.contains(elem)) {
                    return true;
                }
            }
            if (!set.add(elem)) {
                return false;
            }
            if (slots.added_set.valid() && slots.added_set.is_set()) {
                slots.added_set.as_set().add(elem);
            }
            return true;
        };

        auto apply_remove = [&](const View& elem) -> bool {
            if (!elem.valid()) {
                return false;
            }
            if (!set.remove(elem)) {
                return false;
            }
            if (slots.added_set.valid() && slots.added_set.is_set()) {
                auto added = slots.added_set.as_set();
                if (added.contains(elem)) {
                    added.remove(elem);
                    return true;
                }
            }
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().add(elem);
            }
            return true;
        };

        auto apply_add_object = [&](const nb::object& obj) -> bool {
            auto maybe_value = value_from_python(element_type, obj);
            if (!maybe_value.has_value()) {
                return false;
            }
            return apply_add(maybe_value->view());
        };

        auto apply_remove_object = [&](const nb::object& obj) -> bool {
            auto maybe_value = value_from_python(element_type, obj);
            if (!maybe_value.has_value()) {
                return false;
            }
            return apply_remove(maybe_value->view());
        };

        bool changed = false;
        bool handled = false;

        nb::object added_attr = attr_or_call(src, "added");
        nb::object removed_attr = attr_or_call(src, "removed");
        if (!added_attr.is_none() || !removed_attr.is_none()) {
            handled = true;
            if (!removed_attr.is_none()) {
                for (const auto& item : nb::iter(removed_attr)) {
                    changed = apply_remove_object(nb::cast<nb::object>(item)) || changed;
                }
            }
            if (!added_attr.is_none()) {
                for (const auto& item : nb::iter(added_attr)) {
                    changed = apply_add_object(nb::cast<nb::object>(item)) || changed;
                }
            }
        }

        if (!handled && nb::isinstance<nb::dict>(src)) {
            nb::dict as_dict = nb::cast<nb::dict>(src);
            if (as_dict.contains("added") || as_dict.contains("removed")) {
                handled = true;
                if (as_dict.contains("removed")) {
                    for (const auto& item : nb::iter(as_dict["removed"])) {
                        changed = apply_remove_object(nb::cast<nb::object>(item)) || changed;
                    }
                }
                if (as_dict.contains("added")) {
                    for (const auto& item : nb::iter(as_dict["added"])) {
                        changed = apply_add_object(nb::cast<nb::object>(item)) || changed;
                    }
                }
            }
        }

        if (!handled && nb::isinstance<nb::frozenset>(src)) {
            handled = true;

            std::vector<Value> target_values;
            for (const auto& item : nb::iter(src)) {
                auto maybe_value = value_from_python(element_type, nb::cast<nb::object>(item));
                if (maybe_value.has_value()) {
                    target_values.emplace_back(std::move(*maybe_value));
                }
            }

            std::vector<Value> existing_values;
            existing_values.reserve(set.size());
            for (View elem : set) {
                existing_values.emplace_back(elem.clone());
            }

            for (const auto& elem : existing_values) {
                bool keep = false;
                for (const auto& target : target_values) {
                    if (target.view().schema() == elem.view().schema() && target.view().equals(elem.view())) {
                        keep = true;
                        break;
                    }
                }
                if (!keep) {
                    changed = apply_remove(elem.view()) || changed;
                }
            }

            for (const auto& target : target_values) {
                changed = apply_add(target.view()) || changed;
            }
        }

        if (!handled) {
            nb::object removed_cls = get_removed();
            for (const auto& item : nb::iter(src)) {
                nb::object obj = nb::cast<nb::object>(item);
                if (nb::isinstance(obj, removed_cls)) {
                    changed = apply_remove_object(nb::cast<nb::object>(obj.attr("item"))) || changed;
                } else {
                    changed = apply_add_object(obj) || changed;
                }
            }
        }

        const bool preserve_existing_tick =
            handled &&
            !changed &&
            current_time != MIN_DT &&
            direct_last_modified_time(vd) == current_time;
        if (changed || !was_valid || preserve_existing_tick) {
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (dispatch_meta_is_tsl(current)) {
        if (reset_root_value_and_delta_on_none(vd, src, current_time)) {
            return;
        }

        bool changed = false;
        const size_t child_count = op_list_size(vd);

        auto apply_child = [&](size_t index, const nb::object& child_obj) {
            if (child_obj.is_none()) {
                return;
            }
            if (current->fixed_size() > 0 && index >= child_count) {
                return;
            }
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(index);
            op_from_python(child_vd, child_obj, current_time);
            changed = changed || op_modified(child_vd, current_time);
        };

        bool handled = false;
        if (nb::isinstance<nb::dict>(src)) {
            handled = true;
            nb::dict mapping = nb::cast<nb::dict>(src);
            for (const auto& kv : mapping) {
                ssize_t index = nb::cast<ssize_t>(nb::cast<nb::object>(kv.first));
                if (index < 0) {
                    continue;
                }
                apply_child(static_cast<size_t>(index), nb::cast<nb::object>(kv.second));
            }
        } else if (nb::isinstance<nb::list>(src) || nb::isinstance<nb::tuple>(src)) {
            handled = true;
            if (current->fixed_size() > 0) {
                const size_t provided_size = static_cast<size_t>(nb::len(src));
                if (provided_size != child_count) {
                    throw nb::value_error(
                        fmt::format("Expected {} elements, got {}", child_count, provided_size).c_str());
                }
            }
            size_t index = 0;
            for (const auto& item : nb::iter(src)) {
                apply_child(index++, nb::cast<nb::object>(item));
            }
        }

        if (!handled) {
            apply_fallback_from_python_write(vd, src, current_time);
            return;
        }

        notify_if_static_container_children_changed(changed, vd, current_time);
        return;
    }

    if (dispatch_meta_is_tsb(current)) {
        if (reset_root_value_and_delta_on_none(vd, src, current_time)) {
            return;
        }

        bool changed = false;
        auto apply_child = [&](size_t index, const nb::object& child_obj) {
            if (child_obj.is_none()) {
                return;
            }
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(index);
            if (debug_ref_from) {
                const TSMeta* child_meta = meta_at_path(child_vd.meta, child_vd.path.indices);
                std::fprintf(stderr,
                             "[tsb_from] child path=%s kind=%d\n",
                             child_vd.path.to_string().c_str(),
                             child_meta != nullptr ? static_cast<int>(child_meta->kind) : -1);
            }
            op_from_python(child_vd, child_obj, current_time);
            changed = changed || op_modified(child_vd, current_time);
        };

        bool handled = false;
        if (current->fields() != nullptr) {
            nb::object item_attr = nb::getattr(src, "items", nb::none());
            if (!item_attr.is_none()) {
                handled = true;
                nb::iterator items = nb::iter(item_attr());
                for (const auto& kv : items) {
                    std::string field_name = nb::cast<std::string>(nb::cast<nb::object>(kv[0]));
                    const size_t index = find_bundle_field_index(current, field_name);
                    if (index == static_cast<size_t>(-1)) {
                        continue;
                    }
                    if (debug_ref_from) {
                        std::string v_repr{"<repr_error>"};
                        try {
                            v_repr = nb::cast<std::string>(nb::repr(nb::cast<nb::object>(kv[1])));
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsb_from] path=%s field=%s idx=%zu v=%s\n",
                                     vd.path.to_string().c_str(),
                                     field_name.c_str(),
                                     index,
                                     v_repr.c_str());
                    }
                    apply_child(index, nb::cast<nb::object>(kv[1]));
                }
            } else {
                for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
                    nb::object child_obj = nb::getattr(src, field_name, nb::none());
                    if (!child_obj.is_none()) {
                        handled = true;
                        apply_child(i, child_obj);
                    }
                });
            }
        }

        if (!handled) {
            apply_fallback_from_python_write(vd, src, current_time);
            return;
        }

        notify_if_static_container_children_changed(changed, vd, current_time);
        return;
    }

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }

    if (vd.path.indices.empty() && src.is_none()) {
        auto* value_root = static_cast<Value*>(vd.value_data);
        if (value_root != nullptr) {
            value_root->reset();
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (src.is_none()) {
        // Non-root TS assignments of None invalidate the leaf while still
        // ticking parent containers in this cycle.
        maybe_dst->from_python(src);
        stamp_time_paths(vd, current_time);
        set_leaf_time_path(vd, MIN_DT);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    maybe_dst->from_python(src);
    stamp_time_paths(vd, current_time);
    mark_tsd_parent_child_modified(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

}  // namespace hgraph
