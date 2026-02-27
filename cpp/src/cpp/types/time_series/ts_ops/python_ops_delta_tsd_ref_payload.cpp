#include "ts_ops_internal.h"

namespace hgraph {

nb::object tsd_ref_payload_to_python(const TimeSeriesReference& ref,
                                     const TSMeta* element_meta,
                                     engine_time_t current_time,
                                     bool include_unmodified) {
    if (ref.is_empty()) {
        return nb::none();
    }

    if (const ViewData* target = ref.bound_view(); target != nullptr) {
        if (!include_unmodified) {
            if (target->ops == nullptr || !target->ops->modified(*target, current_time)) {
                return nb::none();
            }
            nb::object delta_obj = op_delta_to_python(*target, current_time);
            if (!delta_obj.is_none()) {
                return delta_obj;
            }
            return nb::none();
        }

        if (target->ops != nullptr && target->ops->modified(*target, current_time)) {
            nb::object delta_obj = op_delta_to_python(*target, current_time);
            if (!delta_obj.is_none()) {
                return delta_obj;
            }
        }

        ViewData sampled_target = *target;
        sampled_target.sampled = true;
        nb::object sampled_delta = op_delta_to_python(sampled_target, current_time);
        if (!sampled_delta.is_none()) {
            return sampled_delta;
        }
        return op_to_python(*target);
    }

    if (!ref.is_unbound()) {
        return nb::none();
    }

    const auto& items = ref.items();
    if (dispatch_meta_is_tsb(element_meta) && element_meta->fields() != nullptr) {
        nb::dict out;
        const size_t n = std::min(items.size(), element_meta->field_count());
        for (size_t i = 0; i < n; ++i) {
            const char* field_name = element_meta->fields()[i].name;
            if (field_name == nullptr) {
                continue;
            }
            const TSMeta* field_meta = element_meta->fields()[i].ts_type;
            nb::object item_py = tsd_ref_payload_to_python(items[i], field_meta, current_time, include_unmodified);
            if (!item_py.is_none()) {
                out[nb::str(field_name)] = std::move(item_py);
            }
        }
        return PyDict_Size(out.ptr()) == 0 ? nb::none() : nb::object(out);
    }

    if (dispatch_meta_is_tsl(element_meta)) {
        nb::dict out;
        const TSMeta* child_meta = element_meta->element_ts();
        for (size_t i = 0; i < items.size(); ++i) {
            nb::object item_py = tsd_ref_payload_to_python(items[i], child_meta, current_time, include_unmodified);
            if (!item_py.is_none()) {
                out[nb::int_(i)] = std::move(item_py);
            }
        }
        return PyDict_Size(out.ptr()) == 0 ? nb::none() : nb::object(out);
    }

    if (items.size() == 1) {
        return tsd_ref_payload_to_python(items[0], element_meta, current_time, include_unmodified);
    }

    nb::list out;
    for (const auto& item : items) {
        nb::object item_py = tsd_ref_payload_to_python(item, element_meta, current_time, include_unmodified);
        if (!item_py.is_none()) {
            out.append(std::move(item_py));
        }
    }
    return out.empty() ? nb::none() : nb::object(out);
}

nb::object tsd_ref_view_payload_to_python(const ViewData& ref_child,
                                          const TSMeta* ref_meta,
                                          engine_time_t current_time,
                                          bool include_unmodified,
                                          bool debug_ref_payload) {
    View ref_value = op_value(ref_child);
    if (!ref_value.valid() || ref_value.schema() != ts_reference_meta()) {
        ViewData target{};
        bool has_target = resolve_bound_target_view_data(ref_child, target);
        if (!has_target) {
            if (auto rebound = resolve_bound_view_data(ref_child); rebound.has_value()) {
                target = *rebound;
                has_target = true;
            }
        }

        if (has_target) {
            const bool target_modified =
                target.ops != nullptr && target.ops->modified != nullptr &&
                target.ops->modified(target, current_time);
            if (target_modified) {
                nb::object delta_obj = op_delta_to_python(target, current_time);
                if (!delta_obj.is_none()) {
                    return delta_obj;
                }
            }

            if (include_unmodified) {
                ViewData sampled_target = target;
                sampled_target.sampled = true;
                nb::object sampled_delta = op_delta_to_python(sampled_target, current_time);
                if (!sampled_delta.is_none()) {
                    return sampled_delta;
                }
                nb::object value_obj = op_to_python(target);
                if (!value_obj.is_none()) {
                    return value_obj;
                }
            } else if (target_modified) {
                nb::object value_obj = op_to_python(target);
                if (!value_obj.is_none()) {
                    return value_obj;
                }
            }
        }

        if (debug_ref_payload) {
            std::fprintf(stderr,
                         "[tsd_ref_payload] path=%s now=%lld include=%d ref_valid=0\n",
                         ref_child.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         include_unmodified ? 1 : 0);
        }
        return nb::none();
    }

    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value.to_python());
    const TSMeta* element_meta = ref_meta != nullptr ? ref_meta->element_ts() : nullptr;
    nb::object payload = tsd_ref_payload_to_python(ref, element_meta, current_time, include_unmodified);
    if (debug_ref_payload) {
        std::string payload_s{"<none>"};
        try {
            payload_s = nb::cast<std::string>(nb::repr(payload));
        } catch (...) {}
        std::fprintf(stderr,
                     "[tsd_ref_payload] path=%s now=%lld include=%d ref_kind=%d elem_kind=%d payload=%s\n",
                     ref_child.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     include_unmodified ? 1 : 0,
                     static_cast<int>(ref.kind()),
                     element_meta != nullptr ? static_cast<int>(element_meta->kind) : -1,
                     payload_s.c_str());
    }
    return payload;
}

bool tsd_has_delta_payload_view(const View& view) {
    if (!view.valid()) {
        return false;
    }
    if (view.schema() == ts_reference_meta()) {
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
        return ref.bound_view() != nullptr;
    }
    return true;
}

bool tsd_has_delta_payload(const DeltaView& delta) {
    return tsd_has_delta_payload_view(delta.value());
}

}  // namespace hgraph
