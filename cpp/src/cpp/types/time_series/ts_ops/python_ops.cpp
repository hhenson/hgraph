#include "ts_ops_internal.h"

namespace hgraph {
nb::object op_to_python_ref(const ViewData& vd) {
    if (auto local = resolve_value_slot_const(vd);
        local.has_value() &&
        local->valid() &&
        local->schema() == ts_reference_meta()) {
        TimeSeriesReference local_ref = nb::cast<TimeSeriesReference>(local->to_python());
        // Preserve explicit empty REF payloads instead of re-materializing
        // bound wrappers as non-empty references. Only apply this when there
        // is no distinct bound target; bound REF inputs rely on wrapper
        // materialization from their binding.
        if (local_ref.is_empty()) {
            ViewData bound_target{};
            const bool has_distinct_bound_target =
                resolve_bound_target_view_data(vd, bound_target) &&
                !same_view_identity(bound_target, vd);
            if (!has_distinct_bound_target) {
                return nb::cast(TimeSeriesReference::make());
            }
        }
    }

    // Normalize through op_value() so REF->REF peering does not expose
    // wrapper self-references as payload values.
    View ref_value = op_value(vd);
    if (ref_value.valid() && ref_value.schema() == ts_reference_meta()) {
        return ref_value.to_python();
    }
    if (auto bound = resolve_bound_view_data(vd); bound.has_value()) {
        return nb::cast(TimeSeriesReference::make(*bound));
    }
    return nb::cast(TimeSeriesReference::make());
}

nb::object op_to_python_tsw(const ViewData& vd) {
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return nb::none();
    }
    bind_view_data_ops(resolved);
    const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
    const ts_ops* resolved_ops = resolved.ops != nullptr ? resolved.ops : dispatch_meta_ops(resolved_meta);
    if (resolved_ops != nullptr &&
        resolved_ops->to_python != nullptr &&
        resolved_ops->to_python != &op_to_python_tsw) {
        return resolved_ops->to_python(resolved);
    }
    if (resolved_meta == nullptr) {
        return nb::none();
    }

    if (!op_valid(resolved)) {
        return nb::none();
    }

    View window_value = op_value(resolved);
    if (!window_value.valid()) {
        return nb::none();
    }

    if (dispatch_ops_is_tsw_duration(resolved_ops)) {
        auto* time_root = static_cast<const Value*>(resolved.time_data);
        if (time_root == nullptr || !time_root->has_value()) {
            return nb::none();
        }
        auto time_path = ts_path_to_time_path(resolved.meta, resolved.path.indices);
        if (time_path.empty()) {
            return nb::none();
        }
        time_path.pop_back();
        std::optional<View> maybe_time;
        if (time_path.empty()) {
            maybe_time = time_root->view();
        } else {
            maybe_time = navigate_const(time_root->view(), time_path);
        }
        if (!maybe_time.has_value() || !maybe_time->valid() || !maybe_time->is_tuple()) {
            return nb::none();
        }
        auto tuple = maybe_time->as_tuple();
        if (tuple.size() < 4) {
            return nb::none();
        }
        View ready = tuple.at(3);
        if (!ready.valid() || !ready.is_scalar_type<bool>() || !ready.as<bool>()) {
            return nb::none();
        }
        return window_value.to_python();
    }

    if (!window_value.is_cyclic_buffer()) {
        return nb::none();
    }
    if (window_value.as_cyclic_buffer().size() < resolved_meta->min_period()) {
        return nb::none();
    }
    return window_value.to_python();
}

nb::object op_to_python_tsl(const ViewData& vd) {
    const size_t n = op_list_size(vd);
    nb::list out;
    for (size_t i = 0; i < n; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);
        nb::object child_py = op_valid(child) ? op_to_python(child) : nb::none();
        if (!child_py.is_none()) {
            try {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(child_py);
                if (const ViewData* target = ref.bound_view(); target != nullptr) {
                    child_py = op_to_python(*target);
                } else {
                    child_py = nb::none();
                }
            } catch (...) {
                // Non-reference payload, keep as-is.
            }
        }
        out.append(std::move(child_py));
    }
    return nb::module_::import_("builtins").attr("tuple")(out);
}

nb::object op_to_python_tsb(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    nb::dict out;
    if (current == nullptr || current->fields() == nullptr) {
        return out;
    }

    for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
        ViewData child = vd;
        child.path.indices.push_back(i);
        if (!op_valid(child)) {
            return;
        }
        nb::object child_py = op_to_python(child);
        if (!child_py.is_none()) {
            try {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(child_py);
                if (const ViewData* target = ref.bound_view(); target != nullptr) {
                    child_py = op_to_python(*target);
                } else {
                    child_py = nb::none();
                }
            } catch (...) {
                // Non-reference payload, keep as-is.
            }
        }
        if (!child_py.is_none()) {
            out[nb::str(field_name)] = std::move(child_py);
        }
    });

    // Mirror Python TSB.value semantics: if schema has a scalar_type,
    // materialize that scalar instance from field values.
    try {
        nb::object schema_py = current->python_type();
        if (!schema_py.is_none()) {
            nb::object scalar_type_fn = nb::getattr(schema_py, "scalar_type", nb::none());
            if (!scalar_type_fn.is_none() && PyCallable_Check(scalar_type_fn.ptr()) != 0) {
                nb::object scalar_type = scalar_type_fn();
                if (!scalar_type.is_none()) {
                    nb::dict scalar_kwargs;
                    for_each_named_bundle_field(current, [&](size_t /*i*/, const char* field_name) {
                        nb::object key_obj = nb::str(field_name);
                        if (PyDict_Contains(out.ptr(), key_obj.ptr()) == 1) {
                            scalar_kwargs[key_obj] = out[key_obj];
                        } else {
                            scalar_kwargs[key_obj] = nb::none();
                        }
                    });
                    PyObject* empty_args = PyTuple_New(0);
                    PyObject* scalar_obj = PyObject_Call(scalar_type.ptr(), empty_args, scalar_kwargs.ptr());
                    Py_DECREF(empty_args);
                    if (scalar_obj != nullptr) {
                        return nb::steal<nb::object>(scalar_obj);
                    }
                    PyErr_Clear();
                }
            }
        }
    } catch (...) {
        // Fall back to dict representation when scalar materialization fails.
    }
    return out;
}

nb::object op_to_python_tss(const ViewData& vd) {
    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }

    View v = op_value(vd);
    if (v.valid() && v.is_set()) {
        return v.to_python();
    }
    return nb::frozenset(nb::set{});
}

nb::object op_to_python_tsd(const ViewData& vd) {
    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }

    nb::dict out;
    View v = op_value(vd);
    if (v.valid() && v.is_map()) {
        for_each_map_key_slot(v.as_map(), [&](View key, size_t slot) {
            ViewData child = vd;
            child.path.indices.push_back(slot);
            const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
            if (child_meta == nullptr) {
                if (!op_valid(child)) {
                    return;
                }
                nb::object child_py = op_to_python(child);
                if (child_py.is_none()) {
                    return;
                }
                out[key.to_python()] = std::move(child_py);
                return;
            }

            nb::object child_py = op_to_python(child);
            if (child_py.is_none()) {
                return;
            }
            out[key.to_python()] = std::move(child_py);
        });
    }
    return get_frozendict()(out);
}

nb::object op_to_python(const ViewData& vd) {
    ViewData dispatch_view = vd;
    bind_view_data_ops(dispatch_view);
    const TSMeta* self_meta = meta_at_path(dispatch_view.meta, dispatch_view.path.indices);
    const ts_ops* self_ops = dispatch_meta_ops(self_meta);
    if (self_ops == nullptr) {
        self_ops = dispatch_view.ops;
    }
    if (self_ops != nullptr &&
        self_ops->to_python != nullptr &&
        self_ops->to_python != &op_to_python) {
        return self_ops->to_python(dispatch_view);
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(dispatch_view, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }

    View v = op_value(dispatch_view);
    return v.valid() ? v.to_python() : nb::none();
}

}  // namespace hgraph
