#include "ts_ops_internal.h"

namespace hgraph {

void op_from_python_tss(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = vd.meta;
    if (current == nullptr) {
        return;
    }

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
    if (changed || !was_valid) {
        invalidate_python_value_cache(vd);
        if (nb::object* cache_slot = resolve_python_value_cache_slot(vd, true); cache_slot != nullptr) {
            *cache_slot = maybe_set->to_python();
            vd.python_value_cache_slot = cache_slot;
        }
    }
    if (changed || !was_valid || preserve_existing_tick) {
        stamp_time_paths(vd, current_time);
        notify_link_target_observers(vd, current_time);
    }
}

}  // namespace hgraph
