#include "ts_ops_internal.h"

namespace hgraph {

void op_from_python_tsl(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_FROM_PYTHON")) {
        std::fprintf(stderr, "[from_py_tsl] path=%s depth=%zu level=%p level_depth=%u root_level=%p\n",
                     vd.to_short_path().to_string().c_str(),
                     vd.path_depth(),
                     static_cast<void*>(vd.level),
                     vd.level_depth,
                     static_cast<void*>(vd.root_level));
    }
    const TSMeta* current = vd.meta;
    if (current == nullptr) {
        return;
    }

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
        ViewData child_vd = make_child_view_data(vd, index);
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
        throw nb::type_error("TSL from_python expects dict/list/tuple input");
    }

    if (changed) {
        if (nb::object* cache_slot = resolve_python_value_cache_slot(vd, true); cache_slot != nullptr) {
            auto current_value = resolve_value_slot_const(vd);
            *cache_slot = (current_value.has_value() && current_value->valid()) ? current_value->to_python() : nb::none();
            vd.python_value_cache_slot = cache_slot;
        }
    }
    notify_if_static_container_children_changed(changed, vd, current_time);
}

void op_from_python_tsb(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = vd.meta;
    if (current == nullptr) {
        return;
    }

    const bool debug_ref_from = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_FROM");

    if (reset_root_value_and_delta_on_none(vd, src, current_time)) {
        return;
    }

    bool changed = false;
    auto apply_child = [&](size_t index, const nb::object& child_obj) {
        if (child_obj.is_none()) {
            return;
        }
        ViewData child_vd = make_child_view_data(vd, index);
        if (debug_ref_from) {
            const TSMeta* child_meta = child_vd.meta;
            std::fprintf(stderr,
                         "[tsb_from] child path=%s kind=%d\n",
                         child_vd.to_short_path().to_string().c_str(),
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
                                 vd.to_short_path().to_string().c_str(),
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
        throw nb::type_error("TSB from_python expects mapping or object with matching fields");
    }

    if (changed) {
        if (nb::object* cache_slot = resolve_python_value_cache_slot(vd, true); cache_slot != nullptr) {
            auto current_value = resolve_value_slot_const(vd);
            *cache_slot = (current_value.has_value() && current_value->valid()) ? current_value->to_python() : nb::none();
            vd.python_value_cache_slot = cache_slot;
        }
    }
    notify_if_static_container_children_changed(changed, vd, current_time);
}

}  // namespace hgraph
