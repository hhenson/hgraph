#include "ts_ops_internal.h"

namespace hgraph {

void apply_fallback_from_python_write(ViewData& vd,
                                      const nb::object& src,
                                      engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }
    maybe_dst->from_python(src);
    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void notify_if_static_container_children_changed(bool changed,
                                                 const ViewData& vd,
                                                 engine_time_t current_time) {
    if (!changed) {
        return;
    }

    // Child writes already stamp their own paths (and ancestors). Re-stamping
    // the static container root would mark untouched siblings modified.
    notify_link_target_observers(vd, current_time);
}

nb::object op_to_python(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
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

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }

    const TSMeta* current = self_meta;
    if (current != nullptr && current->kind == TSKind::TSL) {
        const size_t n = op_list_size(vd);
        nb::list out;
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            out.append(op_valid(child) ? op_to_python(child) : nb::none());
        }
        return nb::module_::import_("builtins").attr("tuple")(out);
    }

    if (current != nullptr && current->kind == TSKind::TSB) {
        nb::dict out;
        if (current->fields() == nullptr) {
            return out;
        }
        for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (!op_valid(child)) {
                return;
            }
            out[nb::str(field_name)] = op_to_python(child);
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

    if (current != nullptr && current->kind == TSKind::TSW) {
        ViewData resolved{};
        if (!resolve_read_view_data(vd, current, resolved)) {
            return nb::none();
        }
        const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
        if (resolved_meta == nullptr || resolved_meta->kind != TSKind::TSW) {
            return nb::none();
        }

        if (!op_valid(resolved)) {
            return nb::none();
        }

        View window_value = op_value(resolved);
        if (!window_value.valid()) {
            return nb::none();
        }

        if (resolved_meta->is_duration_based()) {
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

    if (current != nullptr && current->kind == TSKind::TSS) {
        View v = op_value(vd);
        if (v.valid() && v.is_set()) {
            return v.to_python();
        }
        return nb::frozenset(nb::set{});
    }

    if (current != nullptr && current->kind == TSKind::TSD) {
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
                if (child_meta != nullptr && child_meta->kind == TSKind::REF && child_py.is_none()) {
                    // Keep key-space stable internally, but hide unresolved REF entries
                    // from Python-facing value snapshots.
                    return;
                }
                if (child_meta->kind != TSKind::REF && child_py.is_none()) {
                    return;
                }
                out[key.to_python()] = std::move(child_py);
            });
        }
        return get_frozendict()(out);
    }

    View v = op_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_delta_kind = std::getenv("HGRAPH_DEBUG_DELTA_KIND") != nullptr;

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        DeltaView delta = DeltaView::from_computed(vd, current_time);
        // Keep REF delta serialization local here: DeltaView::to_python() would
        // dispatch back to op_delta_to_python() for computed backings.
        return delta.valid() ? delta.value().to_python() : nb::none();
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        if (debug_delta_kind) {
            std::fprintf(stderr,
                         "[delta_kind] keyset path=%s self_kind=%d proj=%d uses_lt=%d now=%lld\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         static_cast<int>(vd.projection),
                         vd.uses_link_target ? 1 : 0,
                         static_cast<long long>(current_time.time_since_epoch().count()));
        }
        if (debug_keyset_bridge) {
            std::fprintf(stderr,
                         "[keyset_delta] direct path=%s self_kind=%d uses_lt=%d source=%s\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         vd.uses_link_target ? 1 : 0,
                         key_set_source.path.to_string().c_str());
        }
        const auto bridge_state = resolve_tsd_key_set_bridge_state(vd, self_meta, current_time);
        if (bridge_state.has_previous_source || bridge_state.has_current_source) {
            if (debug_keyset_bridge) {
                std::fprintf(stderr,
                             "[keyset_delta] bridge path=%s prev=%s curr=%s has_prev=%d has_curr=%d\n",
                             vd.path.to_string().c_str(),
                             (bridge_state.has_previous_source ? bridge_state.previous_source : bridge_state.previous_bridge)
                                 .path.to_string().c_str(),
                             (bridge_state.has_current_source ? bridge_state.current_source : bridge_state.current_bridge)
                                 .path.to_string().c_str(),
                             bridge_state.has_previous_source ? 1 : 0,
                             bridge_state.has_current_source ? 1 : 0);
            }
            if (bridge_state.has_previous_source && !bridge_state.has_current_source) {
                return tsd_key_set_unbind_delta_to_python(bridge_state.previous_source);
            }
            return tsd_key_set_bridge_delta_to_python(
                bridge_state.has_previous_source ? bridge_state.previous_source : bridge_state.previous_bridge,
                bridge_state.has_current_source ? bridge_state.current_source : bridge_state.current_bridge);
        }

        // First bind from empty REF -> concrete TSD has no previous bridge yet.
        // Emit full "added" snapshot so key_set consumers observe immediate adds.
        const bool key_set_projection = is_tsd_key_set_projection(vd);
        if ((self_meta != nullptr && self_meta->kind == TSKind::TSS) || key_set_projection) {
            if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
                is_first_bind_rebind_tick(lt, current_time)) {
                if (debug_keyset_bridge) {
                    std::fprintf(stderr,
                                 "[keyset_delta] first_bind path=%s linked=%d prev=%d rebind=%lld now=%lld\n",
                                 vd.path.to_string().c_str(),
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                }
                return tsd_key_set_all_added_to_python(key_set_source);
            }
        }

        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        return tsd_key_set_delta_to_python(key_set_source);
    }

    const bool key_set_consumer =
        self_meta != nullptr &&
        (self_meta->kind == TSKind::TSS ||
         self_meta->kind == TSKind::SIGNAL ||
         is_tsd_key_set_projection(vd));

    if (key_set_consumer) {
        const auto bridge_state = resolve_tsd_key_set_bridge_state(vd, self_meta, current_time);
        if (bridge_state.has_bridge) {
            if (debug_keyset_bridge) {
                std::fprintf(stderr,
                             "[keyset_delta] fallback path=%s prev_bridge=%s curr_bridge=%s has_prev=%d has_curr=%d\n",
                             vd.path.to_string().c_str(),
                             bridge_state.previous_bridge.path.to_string().c_str(),
                             bridge_state.current_bridge.path.to_string().c_str(),
                             bridge_state.has_previous_source ? 1 : 0,
                             bridge_state.has_current_source ? 1 : 0);
            }
            if (bridge_state.has_previous_source || bridge_state.has_current_source) {
                if (bridge_state.has_previous_source && !bridge_state.has_current_source) {
                    return tsd_key_set_unbind_delta_to_python(bridge_state.previous_source);
                }
                return tsd_key_set_bridge_delta_to_python(
                    bridge_state.has_previous_source ? bridge_state.previous_source : bridge_state.previous_bridge,
                    bridge_state.has_current_source ? bridge_state.current_source : bridge_state.current_bridge);
            }
        } else if (debug_keyset_bridge) {
            if (vd.uses_link_target) {
                if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
                    const TSMeta* target_meta = meta_at_path(lt->meta, lt->target_path.indices);
                    std::fprintf(stderr,
                                 "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d linked=%d prev=%d resolved=%d source_kind=%d last_rebind=%lld now=%lld\n",
                                 vd.path.to_string().c_str(),
                                 self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                                 vd.uses_link_target ? 1 : 0,
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 lt->has_resolved_target ? 1 : 0,
                                 target_meta != nullptr ? static_cast<int>(target_meta->kind) : -1,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
                        for (size_t i = 0; i < 3; ++i) {
                            std::vector<size_t> probe_path = vd.path.indices;
                            probe_path.push_back(i);
                            if (LinkTarget* child_lt = resolve_link_target(vd, probe_path); child_lt != nullptr) {
                                std::fprintf(stderr,
                                             "[keyset_delta] probe_child path=%s idx=%zu child_linked=%d child_kind=%d\n",
                                             vd.path.to_string().c_str(),
                                             i,
                                             child_lt->is_linked ? 1 : 0,
                                             child_lt->meta != nullptr ? static_cast<int>(child_lt->meta->kind) : -1);
                            } else {
                                std::fprintf(stderr,
                                             "[keyset_delta] probe_child path=%s idx=%zu child=<none>\n",
                                             vd.path.to_string().c_str(),
                                             i);
                            }
                        }
                    }
                } else {
                    std::fprintf(stderr,
                                 "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d link_target=<none>\n",
                                 vd.path.to_string().c_str(),
                                 self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                                 vd.uses_link_target ? 1 : 0);
                }
            }
            std::fprintf(stderr,
                         "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         vd.uses_link_target ? 1 : 0);
        }
    }

    const auto delta_view_to_python = [current_time](const View& view) -> nb::object {
        if (!view.valid()) {
            return nb::none();
        }
        if (view.schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                if (target->ops != nullptr) {
                    if (target->ops->modified(*target, current_time)) {
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
                }
                return op_to_python(*target);
            }
            return nb::none();
        }
        return view.to_python();
    };
    const auto computed_delta_to_python = [&delta_view_to_python](const DeltaView& delta) -> nb::object {
        // Do not call DeltaView::to_python() from inside op_delta_to_python():
        // computed backings route through ts_ops::delta_to_python (this function).
        if (!delta.valid()) {
            return nb::none();
        }
        return delta_view_to_python(delta.value());
    };
    const auto stored_delta_to_python = [&computed_delta_to_python](const View& view) -> nb::object {
        return computed_delta_to_python(DeltaView::from_stored(view));
    };
    const auto ref_payload_to_python =
        [current_time](const TimeSeriesReference& ref,
                       const TSMeta* element_meta,
                       bool include_unmodified,
                       const auto& self) -> nb::object {
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
            if (element_meta != nullptr && element_meta->kind == TSKind::TSB && element_meta->fields() != nullptr) {
                nb::dict out;
                const size_t n = std::min(items.size(), element_meta->field_count());
                for (size_t i = 0; i < n; ++i) {
                    const char* field_name = element_meta->fields()[i].name;
                    if (field_name == nullptr) {
                        continue;
                    }
                    const TSMeta* field_meta = element_meta->fields()[i].ts_type;
                    nb::object item_py = self(items[i], field_meta, include_unmodified, self);
                    if (!item_py.is_none()) {
                        out[nb::str(field_name)] = std::move(item_py);
                    }
                }
                return PyDict_Size(out.ptr()) == 0 ? nb::none() : nb::object(out);
            }

            if (element_meta != nullptr && element_meta->kind == TSKind::TSL) {
                nb::dict out;
                const TSMeta* child_meta = element_meta->element_ts();
                for (size_t i = 0; i < items.size(); ++i) {
                    nb::object item_py = self(items[i], child_meta, include_unmodified, self);
                    if (!item_py.is_none()) {
                        out[nb::int_(i)] = std::move(item_py);
                    }
                }
                return PyDict_Size(out.ptr()) == 0 ? nb::none() : nb::object(out);
            }

            if (items.size() == 1) {
                return self(items[0], element_meta, include_unmodified, self);
            }

            nb::list out;
            for (const auto& item : items) {
                nb::object item_py = self(item, element_meta, include_unmodified, self);
                if (!item_py.is_none()) {
                    out.append(std::move(item_py));
                }
            }
            return out.empty() ? nb::none() : nb::object(out);
        };
        const bool debug_ref_payload = std::getenv("HGRAPH_DEBUG_TSD_REF_PAYLOAD") != nullptr;
        const auto ref_view_payload_to_python =
            [&ref_payload_to_python, debug_ref_payload, current_time](const ViewData& ref_child,
                                                                      const TSMeta* ref_meta,
                                                                      bool include_unmodified) -> nb::object {
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
                nb::object payload = ref_payload_to_python(ref, element_meta, include_unmodified, ref_payload_to_python);
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
            };
            const auto has_delta_payload_view = [](const View& view) -> bool {
                if (!view.valid()) {
                    return false;
                }
                if (view.schema() == ts_reference_meta()) {
                    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
                    return ref.bound_view() != nullptr;
                }
                return true;
            };
            const auto has_delta_payload = [&has_delta_payload_view](const DeltaView& delta) -> bool {
                return has_delta_payload_view(delta.value());
            };

    {
        nb::object bridge_delta;
        if (try_container_bridge_delta_to_python(
                vd, self_meta, current_time, true, false, bridge_delta)) {
            return bridge_delta;
        }
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return nb::none();
    }
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (debug_delta_kind) {
        std::fprintf(stderr,
                     "[delta_kind] path=%s self_kind=%d resolved_kind=%d self_proj=%d resolved_proj=%d uses_lt=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     current != nullptr ? static_cast<int>(current->kind) : -1,
                     static_cast<int>(vd.projection),
                     static_cast<int>(data->projection),
                     vd.uses_link_target ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }
    if (current != nullptr && (current->kind == TSKind::TSS || current->kind == TSKind::TSD)) {
        const bool debug_tsd_bridge = std::getenv("HGRAPH_DEBUG_TSD_BRIDGE") != nullptr;
        nb::object bridge_delta;
        if (try_container_bridge_delta_to_python(
                vd, current, current_time, false, debug_tsd_bridge, bridge_delta)) {
            // Python parity: when bindings change, container REF deltas are computed
            // from full previous/current snapshots (not current native delta only).
            return bridge_delta;
        }
    }

    if (current != nullptr && current->kind == TSKind::TSW) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }

        View value_view = op_value(*data);
        auto* time_root = static_cast<const Value*>(data->time_data);
        if (!value_view.valid() || time_root == nullptr || !time_root->has_value()) {
            return nb::none();
        }

        auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
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
        if (tuple.size() < 2) {
            return nb::none();
        }

        if (current->is_duration_based()) {
            if (tuple.size() < 4) {
                return nb::none();
            }
            View ready = tuple.at(3);
            if (!ready.valid() || !ready.is_scalar_type<bool>() || !ready.as<bool>()) {
                return nb::none();
            }
            if (!value_view.is_queue()) {
                return nb::none();
            }
            View time_values = tuple.at(1);
            if (!time_values.valid() || !time_values.is_queue()) {
                return nb::none();
            }

            auto value_queue = value_view.as_queue();
            auto time_queue = time_values.as_queue();
            if (value_queue.size() == 0 || time_queue.size() == 0) {
                return nb::none();
            }

            const auto* newest_time = static_cast<const engine_time_t*>(
                value::QueueOps::get_element_ptr_const(time_queue.data(), time_queue.size() - 1, time_queue.schema()));
            if (newest_time == nullptr || *newest_time != current_time) {
                return nb::none();
            }

            const auto* newest_value = value::QueueOps::get_element_ptr_const(
                value_queue.data(), value_queue.size() - 1, value_queue.schema());
            const value::TypeMeta* element_type = current->value_type;
            if (newest_value == nullptr || element_type == nullptr) {
                return nb::none();
            }
            return element_type->ops().to_python(newest_value, element_type);
        }

        if (!value_view.is_cyclic_buffer()) {
            return nb::none();
        }
        View time_values = tuple.at(1);
        if (!time_values.valid() || !time_values.is_cyclic_buffer()) {
            return nb::none();
        }

        auto value_buffer = value_view.as_cyclic_buffer();
        auto time_buffer = time_values.as_cyclic_buffer();
        if (value_buffer.size() == 0 || time_buffer.size() == 0) {
            return nb::none();
        }

        const auto* newest_time = static_cast<const engine_time_t*>(
            value::CyclicBufferOps::get_element_ptr_const(time_buffer.data(), time_buffer.size() - 1, time_buffer.schema()));
        if (newest_time == nullptr || *newest_time != current_time) {
            return nb::none();
        }

        const auto* newest_value = value::CyclicBufferOps::get_element_ptr_const(
            value_buffer.data(), value_buffer.size() - 1, value_buffer.schema());
        const value::TypeMeta* element_type = current->value_type;
        if (newest_value == nullptr || element_type == nullptr) {
            return nb::none();
        }
        return element_type->ops().to_python(newest_value, element_type);
    }

    if (current != nullptr && current->kind == TSKind::TSS) {
        const bool wrapper_modified = op_modified(vd, current_time);
        const bool resolved_modified = op_modified(*data, current_time);
        if (!wrapper_modified && !resolved_modified) {
            return nb::none();
        }

        nb::set added_set;
        nb::set removed_set;
        bool has_native_delta = false;

        auto* delta_root = static_cast<const Value*>(data->delta_data);
        if (delta_root != nullptr && delta_root->has_value()) {
            std::optional<View> maybe_delta;
            if (auto delta_path = ts_path_to_delta_path(data->meta, data->path.indices); delta_path.has_value()) {
                if (delta_path->empty()) {
                    maybe_delta = delta_root->view();
                } else {
                    maybe_delta = navigate_const(delta_root->view(), *delta_path);
                }
            }

            if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                auto tuple = maybe_delta->as_tuple();
                if (tuple.size() > 0) {
                    View added = tuple.at(0);
                    if (added.valid() && added.is_set()) {
                        for (View elem : added.as_set()) {
                            added_set.add(elem.to_python());
                        }
                        has_native_delta = has_native_delta || added.as_set().size() > 0;
                    }
                }
                if (tuple.size() > 1) {
                    View removed = tuple.at(1);
                    if (removed.valid() && removed.is_set()) {
                        for (View elem : removed.as_set()) {
                            removed_set.add(elem.to_python());
                        }
                        has_native_delta = has_native_delta || removed.as_set().size() > 0;
                    }
                }
            }
        }

        bool sampled_like = data->sampled;
        if (!sampled_like) {
            const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
            if (wrapper_time == current_time && !has_native_delta) {
                sampled_like = true;
            }
        }
        if (sampled_like) {
            added_set = nb::set();
            removed_set = nb::set();
            View current_value = op_value(*data);
            if (current_value.valid()) {
                if (current_value.is_set()) {
                    for (View elem : current_value.as_set()) {
                        added_set.add(elem.to_python());
                    }
                } else if (current_value.is_map()) {
                    for (View key : current_value.as_map().keys()) {
                        added_set.add(key.to_python());
                    }
                }
            }
        }

        return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
    }

    if (current != nullptr && current->kind == TSKind::TSD) {
        const bool debug_tsd_delta = std::getenv("HGRAPH_DEBUG_TSD_DELTA") != nullptr;
        const bool wrapper_modified = op_modified(vd, current_time);
        const bool resolved_modified = op_modified(*data, current_time);
        if (!wrapper_modified && !resolved_modified) {
            if (debug_tsd_delta) {
                std::fprintf(stderr,
                             "[tsd_delta_dbg] path=%s wrapper_modified=0 resolved_modified=0 now=%lld\n",
                             vd.path.to_string().c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()));
            }
            // Non-scalar delta contract: containers return empty payloads, not None.
            return get_frozendict()(nb::dict{});
        }

        nb::dict delta_out;
        View changed_values;
        View added_keys;
        View removed_keys;
        auto* delta_root = static_cast<const Value*>(data->delta_data);
        if (delta_root != nullptr && delta_root->has_value()) {
            std::optional<View> maybe_delta;
            if (auto delta_path = ts_path_to_delta_path(data->meta, data->path.indices); delta_path.has_value()) {
                if (delta_path->empty()) {
                    maybe_delta = delta_root->view();
                } else {
                    maybe_delta = navigate_const(delta_root->view(), *delta_path);
                }
            }

            if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                auto tuple = maybe_delta->as_tuple();
                if (tuple.size() > 0) {
                    changed_values = tuple.at(0);
                }
                if (tuple.size() > 1) {
                    added_keys = tuple.at(1);
                }
                if (tuple.size() > 2) {
                    removed_keys = tuple.at(2);
                }
            }
        }

        auto current_value = resolve_value_slot_const(*data);
        if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
            const auto value_map = current_value->as_map();
            const TSMeta* element_meta = current->element_ts();
            const bool declared_ref_element =
                self_meta != nullptr &&
                self_meta->kind == TSKind::TSD &&
                self_meta->element_ts() != nullptr &&
                self_meta->element_ts()->kind == TSKind::REF;
            const bool nested_element = element_meta != nullptr && !is_scalar_like_ts_kind(element_meta->kind);

            const engine_time_t rebind_time = rebind_time_for_view(vd);
            const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
            const bool has_changed_map =
                changed_values.valid() && changed_values.is_map() && changed_values.as_map().size() > 0;
            const bool single_changed_key =
                changed_values.valid() && changed_values.is_map() && changed_values.as_map().size() == 1;
            const bool has_added_keys =
                added_keys.valid() && added_keys.is_set() && added_keys.as_set().size() > 0;
            const bool has_removed_keys =
                removed_keys.valid() && removed_keys.is_set() && removed_keys.as_set().size() > 0;
            const auto key_in_added_set = [&added_keys](View key) {
                return view_is_set_and_contains_key_relaxed(added_keys, key);
            };
            const auto key_in_removed_set = [&removed_keys](View key) {
                return view_is_set_and_contains_key_relaxed(removed_keys, key);
            };
            const auto key_in_changed_map = [&changed_values](View key) {
                return changed_values.valid() &&
                       changed_values.is_map() &&
                       map_slot_for_key(changed_values.as_map(), key).has_value();
            };
            const auto ref_target_modified_this_tick = [&](const ViewData& ref_child) -> bool {
                ViewData target{};
                return resolve_bound_target_view_data(ref_child, target) &&
                       op_modified(target, current_time);
            };

            bool sampled_like = data->sampled;
            if (!sampled_like && wrapper_modified && !resolved_modified && !has_changed_map) {
                sampled_like = true;
            }
            if (!sampled_like &&
                vd.uses_link_target &&
                rebind_time == current_time) {
                if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices); link_target != nullptr) {
                    // First bind should sample current visible values and avoid carrying
                    // stale remove markers from an unrelated previous binding.
                    sampled_like = !link_target->has_previous_target;
                }
            }
            if (!sampled_like && wrapper_time == current_time && !has_changed_map) {
                sampled_like = true;
            }
            if (!sampled_like &&
                element_meta != nullptr &&
                element_meta->kind == TSKind::REF &&
                vd.uses_link_target &&
                vd.path.port_type == PortType::OUTPUT &&
                resolved_modified &&
                !has_changed_map &&
                !has_added_keys &&
                !has_removed_keys) {
                sampled_like = true;
            }

            const auto resolve_previous_map_view =
                [&](ViewData& previous, value::MapView& previous_map) -> bool {
                    if (!resolve_previous_bound_target_view_data(vd, previous)) {
                        if (!resolve_previous_bound_target_view_data(*data, previous)) {
                            return false;
                        }
                    }
                    auto previous_value = resolve_value_slot_const(previous);
                    if (!previous_value.has_value() || !previous_value->valid() || !previous_value->is_map()) {
                        return false;
                    }
                    previous_map = previous_value->as_map();
                    return true;
                };
            const auto previous_map_entry_visible =
                [&](const ViewData& previous, const value::MapView& previous_map, const View& key) -> bool {
                    auto previous_slot = map_slot_for_key(previous_map, key);
                    if (!previous_slot.has_value()) {
                        return false;
                    }

                    ViewData previous_child = previous;
                    previous_child.path.indices.push_back(*previous_slot);
                    const TSMeta* previous_child_meta = meta_at_path(previous_child.meta, previous_child.path.indices);
                    if (previous_child_meta != nullptr && previous_child_meta->kind == TSKind::REF) {
                        nb::object payload = ref_view_payload_to_python(previous_child, previous_child_meta, true);
                        if (payload.is_none()) {
                            View previous_entry = previous_map.at(key);
                            if (previous_entry.valid() && previous_entry.schema() == ts_reference_meta()) {
                                TimeSeriesReference previous_ref = nb::cast<TimeSeriesReference>(previous_entry.to_python());
                                payload = ref_payload_to_python(
                                    previous_ref,
                                    previous_child_meta->element_ts(),
                                    true,
                                    ref_payload_to_python);
                            }
                        }
                        if (!payload.is_none()) {
                            return true;
                        }
                        const TSMeta* ref_element_meta = previous_child_meta->element_ts();
                        const bool ref_targets_container =
                            ref_element_meta != nullptr &&
                            (ref_element_meta->kind == TSKind::TSD ||
                             ref_element_meta->kind == TSKind::TSS ||
                             ref_element_meta->kind == TSKind::TSB ||
                             ref_element_meta->kind == TSKind::TSL);
                        if (debug_tsd_delta && payload.is_none()) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] ref_prev_visibility path=%s key=%s ref_elem_kind=%d container=%d\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str(),
                                         ref_element_meta != nullptr ? static_cast<int>(ref_element_meta->kind) : -1,
                                         ref_targets_container ? 1 : 0);
                        }
                        if (ref_targets_container) {
                            return true;
                        }
                    }
                    if (!op_valid(previous_child)) {
                        return false;
                    }
                    return !op_to_python(previous_child).is_none();
                };
            const auto ref_binding_changed_from_previous = [&](View key) -> bool {
                ViewData previous{};
                value::MapView previous_map;
                if (!resolve_previous_map_view(previous, previous_map)) {
                    return false;
                }
                auto current_slot = map_slot_for_key(value_map, key);
                auto previous_slot = map_slot_for_key(previous_map, key);
                if (!current_slot.has_value()) {
                    return previous_slot.has_value();
                }
                if (!previous_slot.has_value()) {
                    return true;
                }

                ViewData current_child = *data;
                current_child.path.indices.push_back(*current_slot);
                ViewData previous_child = previous;
                previous_child.path.indices.push_back(*previous_slot);

                ViewData current_target{};
                ViewData previous_target{};
                const bool current_has_target = resolve_bound_target_view_data(current_child, current_target);
                const bool previous_has_target = resolve_bound_target_view_data(previous_child, previous_target);
                if (current_has_target != previous_has_target) {
                    return true;
                }
                if (current_has_target && previous_has_target) {
                    return !same_view_identity(current_target, previous_target);
                }

                View current_entry = value_map.at(key);
                View previous_entry = previous_map.at(key);
                if (!current_entry.valid() || !previous_entry.valid()) {
                    return current_entry.valid() != previous_entry.valid();
                }
                if (current_entry.schema() != ts_reference_meta() ||
                    previous_entry.schema() != ts_reference_meta()) {
                    return current_entry.schema() != previous_entry.schema();
                }

                try {
                    TimeSeriesReference current_ref = nb::cast<TimeSeriesReference>(current_entry.to_python());
                    TimeSeriesReference previous_ref = nb::cast<TimeSeriesReference>(previous_entry.to_python());

                    const ViewData* current_bound = current_ref.bound_view();
                    const ViewData* previous_bound = previous_ref.bound_view();
                    if (current_bound != nullptr || previous_bound != nullptr) {
                        if (current_bound == nullptr || previous_bound == nullptr) {
                            return true;
                        }
                        return !same_view_identity(*current_bound, *previous_bound);
                    }

                    return !(current_ref == previous_ref);
                } catch (...) {
                    return false;
                }
            };
            const auto ref_child_rebound_for_key = [&](const ViewData& child, View key) -> bool {
                return ref_child_rebound_this_tick(child) || ref_binding_changed_from_previous(key);
            };

            if (!sampled_like && removed_keys.valid() && removed_keys.is_set()) {
                auto set = removed_keys.as_set();
                const bool has_added_set = added_keys.valid() && added_keys.is_set();
                nb::object remove = get_remove();
                const auto key_visible_in_previous_view = [&](const View& key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
                        if (element_meta != nullptr && element_meta->kind == TSKind::REF && vd.uses_link_target) {
                            return false;
                        }
                        return true;
                    }
                    return previous_map_entry_visible(previous, previous_map, key);
                };
                const auto key_present_in_previous_map = [&](const View& key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        return false;
                    }
                    return map_slot_for_key(previous_map, key).has_value();
                };
                const auto key_visible_in_removed_snapshot = [&](const View& key) -> bool {
                    auto snapshot = resolve_tsd_removed_child_snapshot(*data, key, current_time);
                    if (!snapshot.has_value()) {
                        return false;
                    }

                    ViewData snapshot_view = snapshot->view_data();
                    const TSMeta* snapshot_meta = snapshot->ts_meta();
                    if (snapshot_meta != nullptr && snapshot_meta->kind == TSKind::REF) {
                        nb::object payload = ref_view_payload_to_python(snapshot_view, snapshot_meta, true);
                        return !payload.is_none();
                    }

                    if (!op_valid(snapshot_view)) {
                        return false;
                    }
                    return !op_to_python(snapshot_view).is_none();
                };
                const auto removed_snapshot_ref_target_written = [&](const View& key) -> bool {
                    auto snapshot = resolve_tsd_removed_child_snapshot(*data, key, current_time);
                    if (!snapshot.has_value()) {
                        return false;
                    }

                    ViewData snapshot_view = snapshot->view_data();
                    ViewData bound_target{};
                    if (!resolve_bound_target_view_data(snapshot_view, bound_target)) {
                        return false;
                    }
                    return op_last_modified_time(bound_target) > MIN_DT;
                };
                for (View key : set) {
                    const bool in_added_set = key_in_added_set(key);
                    const bool in_changed_map = key_in_changed_map(key);
                    const bool in_removed_set = key_in_removed_set(key);
                    const bool seen_visible_before = has_tsd_visible_key_history(*data, key);
                    if (debug_tsd_delta) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] remove_probe path=%s key=%s has_added=%d in_added=%d in_removed=%d in_changed=%d seen_visible=%d\n",
                                     vd.path.to_string().c_str(),
                                     key.to_string().c_str(),
                                     has_added_set ? 1 : 0,
                                     in_added_set ? 1 : 0,
                                     in_removed_set ? 1 : 0,
                                     in_changed_map ? 1 : 0,
                                     seen_visible_before ? 1 : 0);
                    }
                    const bool was_visible = key_visible_in_previous_view(key);
                    const bool ref_link_target_input =
                        element_meta != nullptr &&
                        element_meta->kind == TSKind::REF &&
                        vd.uses_link_target;
                    if (in_added_set &&
                        in_removed_set &&
                        !in_changed_map &&
                        !ref_link_target_input &&
                        !seen_visible_before) {
                        if (debug_tsd_delta) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] remove_skip_structural_unseen path=%s key=%s\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        continue;
                    }
                    if (in_added_set &&
                        !in_changed_map &&
                        !was_visible &&
                        !key_present_in_previous_map(key) &&
                        !ref_link_target_input) {
                        if (debug_tsd_delta) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] remove_skip_added_removed path=%s key=%s\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        continue;
                    }
                    if (!was_visible) {
                        if (in_added_set && key_present_in_previous_map(key)) {
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_emit_added_prev_present path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                            delta_out[key.to_python()] = remove;
                            continue;
                        }
                        if (!in_added_set && key_present_in_previous_map(key)) {
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_emit_prev_present path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                            delta_out[key.to_python()] = remove;
                            continue;
                        }
                        if (ref_link_target_input && !in_changed_map) {
                            const bool visible_in_snapshot = key_visible_in_removed_snapshot(key);
                            const bool target_written =
                                in_added_set && !visible_in_snapshot && removed_snapshot_ref_target_written(key);
                            const bool seen_visible_before =
                                in_added_set && !visible_in_snapshot && has_tsd_visible_key_history(*data, key);
                            if (!in_added_set || visible_in_snapshot || target_written || seen_visible_before) {
                                if (debug_tsd_delta) {
                                    std::fprintf(stderr,
                                                 "[tsd_delta_dbg] remove_emit_ref_link_target path=%s key=%s snapshot_visible=%d target_written=%d seen_visible=%d\n",
                                                 vd.path.to_string().c_str(),
                                                 key.to_string().c_str(),
                                                 visible_in_snapshot ? 1 : 0,
                                                 target_written ? 1 : 0,
                                                 seen_visible_before ? 1 : 0);
                                }
                                delta_out[key.to_python()] = remove;
                                continue;
                            }
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_skip_ref_link_target_invisible path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                        }
                        const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
                        const TSMeta* ref_element_meta =
                            element_meta != nullptr && element_meta->kind == TSKind::REF
                                ? element_meta->element_ts()
                                : nullptr;
                        const bool ref_targets_container =
                            ref_element_meta != nullptr &&
                            (ref_element_meta->kind == TSKind::TSD ||
                             ref_element_meta->kind == TSKind::TSS ||
                             ref_element_meta->kind == TSKind::TSB ||
                             ref_element_meta->kind == TSKind::TSL);
                        if (ref_targets_container) {
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_emit_container_ref path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                            delta_out[key.to_python()] = remove;
                            continue;
                        }
                        if (debug_tsd_delta) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] remove_skip_not_visible path=%s key=%s\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        continue;
                    }
                    if (debug_tsd_delta) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] remove_emit path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     key.to_string().c_str());
                    }
                    delta_out[key.to_python()] = remove;
                }
            }

            if (debug_tsd_delta) {
                size_t changed_size = 0;
                size_t added_size = 0;
                size_t removed_size = 0;
                if (changed_values.valid() && changed_values.is_map()) {
                    changed_size = changed_values.as_map().size();
                }
                if (added_keys.valid() && added_keys.is_set()) {
                    added_size = added_keys.as_set().size();
                }
                if (removed_keys.valid() && removed_keys.is_set()) {
                    removed_size = removed_keys.as_set().size();
                }
                std::fprintf(stderr,
                             "[tsd_delta_dbg] path=%s kind=%d elem=%d modified=1 sampled=%d sampled_like=%d rebind=%lld wrapper=%lld changed=%zu added=%zu removed=%zu now=%lld\n",
                             vd.path.to_string().c_str(),
                             static_cast<int>(current->kind),
                             element_meta != nullptr ? static_cast<int>(element_meta->kind) : -1,
                             data->sampled ? 1 : 0,
                             sampled_like ? 1 : 0,
                             static_cast<long long>(rebind_time.time_since_epoch().count()),
                             static_cast<long long>(wrapper_time.time_since_epoch().count()),
                             changed_size,
                             added_size,
                             removed_size,
                             static_cast<long long>(current_time.time_since_epoch().count()));
                if (added_keys.valid() && added_keys.is_set()) {
                    for (View dbg_key : added_keys.as_set()) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] added_key path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     dbg_key.to_string().c_str());
                    }
                }
                if (removed_keys.valid() && removed_keys.is_set()) {
                    for (View dbg_key : removed_keys.as_set()) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] removed_key path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     dbg_key.to_string().c_str());
                    }
                }
                if (changed_values.valid() && changed_values.is_map()) {
                    for (View dbg_key : changed_values.as_map().keys()) {
                        std::string key_s{"<key>"};
                        std::string val_s{"<value>"};
                        bool entry_valid = false;
                        bool entry_is_map = false;
                        size_t entry_map_size = 0;
                        try {
                            key_s = dbg_key.to_string();
                        } catch (...) {}
                        try {
                            View entry = changed_values.as_map().at(dbg_key);
                            entry_valid = entry.valid();
                            entry_is_map = entry.valid() && entry.is_map();
                            entry_map_size = entry_is_map ? entry.as_map().size() : 0;
                            val_s = entry.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] changed_entry path=%s key=%s valid=%d is_map=%d map_size=%zu value=%s\n",
                                     vd.path.to_string().c_str(),
                                     key_s.c_str(),
                                     entry_valid ? 1 : 0,
                                     entry_is_map ? 1 : 0,
                                     entry_map_size,
                                     val_s.c_str());
                    }
                }
            }

            if (!sampled_like && has_changed_map) {
                const auto changed_map = changed_values.as_map();
                const bool debug_changed_map = std::getenv("HGRAPH_DEBUG_TSD_CHANGED_MAP") != nullptr;
                for_each_map_key_slot(value_map, [&](View key, size_t /*slot_index*/) {
                    if (!map_slot_for_key(changed_map, key).has_value()) {
                        return;
                    }
                    auto slot = map_slot_for_key(value_map, key);
                    if (!slot.has_value()) {
                        if (debug_changed_map) {
                            std::fprintf(stderr,
                                         "[tsd_changed_map] path=%s key=%s slot=<none>\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        return;
                    }
                    View changed_entry = changed_map.at(key);
                    DeltaView changed_entry_delta = DeltaView::from_stored(changed_entry);
                    const bool changed_entry_has_delta = has_delta_payload(changed_entry_delta);

                    ViewData child = *data;
                    child.path.indices.push_back(*slot);
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);

                    if (nested_element) {
                        const bool child_valid = op_valid(child);
                        const bool ref_child_rebound =
                            child_meta != nullptr &&
                            child_meta->kind == TSKind::REF &&
                            ref_child_rebound_for_key(child, key);
                        const bool ref_target_modified_now =
                            child_meta != nullptr &&
                            child_meta->kind == TSKind::REF &&
                            ref_target_modified_this_tick(child);
                        const bool include_unmodified_ref_payload =
                            key_in_added_set(key) ||
                            !child_valid ||
                            ref_child_rebound ||
                            !ref_target_modified_now ||
                            !changed_entry_has_delta;
                        if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                            nb::object child_delta =
                                ref_view_payload_to_python(child, child_meta, include_unmodified_ref_payload);
                            if (debug_changed_map) {
                                std::fprintf(stderr,
                                             "[tsd_changed_map] path=%s key=%s slot=%zu child_kind=REF include_unmod=%d out_none=%d\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str(),
                                             *slot,
                                             include_unmodified_ref_payload ? 1 : 0,
                                             child_delta.is_none() ? 1 : 0);
                            }
                            if (!child_delta.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta);
                            }
                            return;
                        }
                        if (!child_valid) {
                            if (debug_changed_map) {
                                std::fprintf(stderr,
                                             "[tsd_changed_map] path=%s key=%s slot=%zu child_valid=0\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str(),
                                             *slot);
                            }
                            return;
                        }
                        const bool include_unmodified_nested_payload = key_in_added_set(key);
                        nb::object child_delta;
                        if (include_unmodified_nested_payload) {
                            // Added nested keys should emit full visible snapshots,
                            // not only the nested native delta payload.
                            ViewData sampled_child = child;
                            sampled_child.sampled = true;
                            child_delta = op_delta_to_python(sampled_child, current_time);
                            if (child_delta.is_none()) {
                                child_delta = op_to_python(child);
                            }
                        } else {
                            child_delta = op_delta_to_python(child, current_time);
                        }
                        if (debug_changed_map) {
                            std::fprintf(stderr,
                                         "[tsd_changed_map] path=%s key=%s slot=%zu child_kind=%d include_unmod=%d out_none=%d\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str(),
                                         *slot,
                                         child_meta != nullptr ? static_cast<int>(child_meta->kind) : -1,
                                         include_unmodified_nested_payload ? 1 : 0,
                                         child_delta.is_none() ? 1 : 0);
                        }
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                    } else {
                        const bool child_valid = op_valid(child);
                        if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                            const bool include_unmodified_ref_payload =
                                key_in_added_set(key) ||
                                !child_valid ||
                                ref_child_rebound_for_key(child, key) ||
                                !has_tsd_visible_key_history(*data, key);
                            nb::object child_delta_py =
                                ref_view_payload_to_python(child, child_meta, include_unmodified_ref_payload);
                            const TSMeta* ref_element_meta = child_meta->element_ts();
                            const bool scalar_ref_target =
                                ref_element_meta != nullptr && is_scalar_like_ts_kind(ref_element_meta->kind);
                            if (child_delta_py.is_none() &&
                                !include_unmodified_ref_payload &&
                                changed_entry_has_delta &&
                                scalar_ref_target &&
                                vd.path.port_type == PortType::INPUT &&
                                !has_added_keys &&
                                !has_removed_keys &&
                                single_changed_key) {
                                child_delta_py = ref_view_payload_to_python(child, child_meta, true);
                            }
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                            }
                            return;
                        }
                        if (!child_valid) {
                            return;
                        }
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (has_delta_payload(child_delta)) {
                            nb::object child_delta_py = computed_delta_to_python(child_delta);
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                            }
                            return;
                        }

                        // changed_values already identified an effective visible value change
                        // (for example precedence/carry updates). Emit current value when the
                        // scalar child has no native delta payload.
                        View child_value = op_value(child);
                        if (!child_value.valid()) {
                            return;
                        }
                        nb::object child_value_py = stored_delta_to_python(child_value);
                        if (!child_value_py.is_none()) {
                            delta_out[key.to_python()] = std::move(child_value_py);
                        }
                    }
                });
            } else {
                const bool include_unmodified = sampled_like;
                const auto key_visible_in_previous_map = [&](View key) -> std::optional<bool> {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        return std::nullopt;
                    }
                    return previous_map_entry_visible(previous, previous_map, key);
                };
                const auto key_present_in_previous_map_for_sample = [&](View key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        return false;
                    }
                    return map_slot_for_key(previous_map, key).has_value();
                };
                const auto key_is_structural_add = [&](View key) {
                    if (!key_in_added_set(key)) {
                        return false;
                    }
                    if (key_in_removed_set(key)) {
                        return false;
                    }
                    const auto was_visible = key_visible_in_previous_map(key);
                    if (!was_visible.has_value()) {
                        return false;
                    }
                    return !(*was_visible);
                };
                for_each_map_key_slot(value_map, [&](View key, size_t slot) {
                    if (debug_tsd_delta) {
                        bool entry_valid = false;
                        bool entry_is_map = false;
                        size_t entry_map_size = 0;
                        std::string key_s{"<key>"};
                        std::string entry_s{"<entry>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        try {
                            View entry = value_map.at(key);
                            entry_valid = entry.valid();
                            entry_is_map = entry.valid() && entry.is_map();
                            entry_map_size = entry_is_map ? entry.as_map().size() : 0;
                            entry_s = entry.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] value_entry path=%s key=%s slot=%zu valid=%d is_map=%d map_size=%zu value=%s\n",
                                     data->path.to_string().c_str(),
                                     key_s.c_str(),
                                     slot,
                                     entry_valid ? 1 : 0,
                                     entry_is_map ? 1 : 0,
                                     entry_map_size,
                                     entry_s.c_str());
                    }
                    ViewData child = *data;
                    child.path.indices.push_back(slot);
                    child.sampled = false;
                    if (include_unmodified) {
                        const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                        const bool child_valid = op_valid(child);
                        if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                            nb::object entry_py = nb::none();
                            if (child_valid) {
                                entry_py = ref_view_payload_to_python(child, child_meta, true);
                            }
                            if (entry_py.is_none()) {
                                if (declared_ref_element) {
                                    View current_entry = value_map.at(key);
                                    if (current_entry.valid() && current_entry.schema() == ts_reference_meta()) {
                                        entry_py = current_entry.to_python();
                                    }
                                }
                            }
                            if (entry_py.is_none()) {
                                if (key_present_in_previous_map_for_sample(key) || !key_in_added_set(key)) {
                                    entry_py = get_remove();
                                }
                            }
                            if (entry_py.is_none()) {
                                return;
                            }
                            delta_out[key.to_python()] = std::move(entry_py);
                            return;
                        }
                        if (!child_valid) {
                            return;
                        }

                        ViewData sampled_child = child;
                        sampled_child.sampled = true;
                        nb::object entry_py = op_delta_to_python(sampled_child, current_time);
                        if (entry_py.is_none()) {
                            entry_py = op_to_python(child);
                        }
                        if (entry_py.is_none()) {
                            return;
                        }
                        delta_out[key.to_python()] = std::move(entry_py);
                        return;
                    }
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                    const bool child_valid = op_valid(child);
                    if (!child_valid &&
                        !(child_meta != nullptr && child_meta->kind == TSKind::REF)) {
                        return;
                    }
                    if (debug_tsd_delta && !include_unmodified) {
                        const engine_time_t child_last = op_last_modified_time(child);
                        std::string key_s{"<key>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] child_probe path=%s key=%s slot=%zu last=%lld now=%lld\n",
                                     child.path.to_string().c_str(),
                                     key_s.c_str(),
                                     slot,
                                     static_cast<long long>(child_last.time_since_epoch().count()),
                                     static_cast<long long>(current_time.time_since_epoch().count()));
                    }
                    const bool forced_from_changed_map = !include_unmodified && key_in_changed_map(key);
                    const bool forced_from_structural_add =
                        !include_unmodified &&
                        (key_is_structural_add(key) || (key_in_added_set(key) && !key_in_removed_set(key)));
                    const bool ref_child_rebound =
                        !include_unmodified &&
                        child_meta != nullptr &&
                        child_meta->kind == TSKind::REF &&
                        ref_child_rebound_for_key(child, key);
                    bool child_modified =
                        include_unmodified || forced_from_changed_map || forced_from_structural_add || ref_child_rebound ||
                        op_modified(child, current_time);
                    if (debug_tsd_delta && !include_unmodified) {
                        std::string key_s{"<key>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] child_flags path=%s key=%s child_kind=%d in_added=%d in_removed=%d forced_changed=%d forced_add=%d rebound=%d\n",
                                     child.path.to_string().c_str(),
                                     key_s.c_str(),
                                     child_meta != nullptr ? static_cast<int>(child_meta->kind) : -1,
                                     key_in_added_set(key) ? 1 : 0,
                                     key_in_removed_set(key) ? 1 : 0,
                                     forced_from_changed_map ? 1 : 0,
                                     forced_from_structural_add ? 1 : 0,
                                     ref_child_rebound ? 1 : 0);
                    }
                    if (!include_unmodified && !child_modified &&
                        child_meta != nullptr && child_meta->kind == TSKind::REF) {
                        child_modified = ref_target_modified_this_tick(child);
                    }
                    if (!include_unmodified && !child_modified) {
                        return;
                    }
                    if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                        nb::object child_delta =
                            ref_view_payload_to_python(
                                child,
                                child_meta,
                                include_unmodified || forced_from_changed_map || forced_from_structural_add ||
                                    ref_child_rebound);
                        if (child_delta.is_none()) {
                            const auto was_visible = key_visible_in_previous_map(key);
                            if (was_visible.has_value() && *was_visible) {
                                child_delta = get_remove();
                            }
                        }
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                        return;
                    }
                    if (child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind)) {
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (has_delta_payload(child_delta)) {
                            nb::object child_delta_py = nb::none();
                            if (child_delta.valid() && child_delta.schema() == ts_reference_meta()) {
                                child_delta_py = ref_view_payload_to_python(
                                    child,
                                    nullptr,
                                    forced_from_changed_map || forced_from_structural_add);
                            } else {
                                child_delta_py = computed_delta_to_python(child_delta);
                            }
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                                return;
                            }
                        }
                        if (forced_from_changed_map || forced_from_structural_add) {
                            View child_value = op_value(child);
                            if (child_value.valid()) {
                                nb::object child_value_py = nb::none();
                                if (child_value.schema() == ts_reference_meta()) {
                                    child_value_py = ref_view_payload_to_python(child, nullptr, true);
                                } else {
                                    child_value_py = stored_delta_to_python(child_value);
                                }
                                if (!child_value_py.is_none()) {
                                    delta_out[key.to_python()] = std::move(child_value_py);
                                }
                            }
                        }
                    } else {
                        nb::object child_delta = nb::none();
                        if (forced_from_structural_add) {
                            // Structural adds should materialize the full visible child payload.
                            child_delta = op_to_python(child);
                            if (child_delta.is_none()) {
                                ViewData sampled_child = child;
                                sampled_child.sampled = true;
                                child_delta = op_delta_to_python(sampled_child, current_time);
                            }
                        } else {
                            child_delta = op_delta_to_python(child, current_time);
                            if (child_delta.is_none() && forced_from_changed_map) {
                                child_delta = op_to_python(child);
                            }
                        }
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                    }
                });
            }

            // Ensure changed keys materialize visible payloads even when child-native
            // deltas are empty (for example REF rebinding/carry-forward updates).
            if (!sampled_like && changed_values.valid() && changed_values.is_map()) {
                const auto changed_map = changed_values.as_map();
                for (View key : changed_map.keys()) {
                    nb::object py_key = key.to_python();
                    if (PyDict_Contains(delta_out.ptr(), py_key.ptr()) == 1) {
                        continue;
                    }
                    auto slot = map_slot_for_key(value_map, key);
                    if (!slot.has_value()) {
                        continue;
                    }
                    ViewData child = *data;
                    child.path.indices.push_back(*slot);
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                    const bool in_added_set = key_in_added_set(key);
                    const bool ref_child_rebound =
                        child_meta != nullptr &&
                        child_meta->kind == TSKind::REF &&
                        ref_child_rebound_for_key(child, key);
                    bool child_modified_now = op_modified(child, current_time);
                    if (!child_modified_now &&
                        child_meta != nullptr &&
                        child_meta->kind == TSKind::REF) {
                        child_modified_now = ref_target_modified_this_tick(child);
                    }
                    const bool child_valid = op_valid(child);
                    if (!in_added_set &&
                        !child_modified_now &&
                        !ref_child_rebound &&
                        child_valid) {
                        continue;
                    }

                    nb::object entry = nb::none();
                    if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                        entry = ref_view_payload_to_python(
                            child,
                            child_meta,
                            in_added_set ||
                                ref_child_rebound ||
                                !child_valid);
                    } else if (child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind)) {
                        if (!child_valid) {
                            continue;
                        }
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (has_delta_payload(child_delta)) {
                            entry = computed_delta_to_python(child_delta);
                        }
                        if (entry.is_none()) {
                            View child_value = op_value(child);
                            if (child_value.valid()) {
                                entry = stored_delta_to_python(child_value);
                            }
                        }
                    } else {
                        if (!child_valid) {
                            continue;
                        }
                        entry = op_delta_to_python(child, current_time);
                        if (entry.is_none()) {
                            entry = op_to_python(child);
                        }
                    }

                    if (!entry.is_none()) {
                        delta_out[std::move(py_key)] = std::move(entry);
                    }
                }
            }

        }

        const auto is_empty_mapping_payload = [](const nb::object& value_obj) {
            if (value_obj.is_none()) {
                return false;
            }
            if (nb::isinstance<nb::dict>(value_obj)) {
                return nb::len(value_obj) == 0;
            }
            nb::object items = nb::getattr(value_obj, "items", nb::none());
            if (items.is_none() || PyCallable_Check(items.ptr()) == 0) {
                return false;
            }
            nb::object iter_items = items();
            if (iter_items.is_none()) {
                return false;
            }
            Py_ssize_t size = PyObject_Length(iter_items.ptr());
            if (size < 0) {
                PyErr_Clear();
                return false;
            }
            return size == 0;
        };

        if (PyDict_Size(delta_out.ptr()) > 0) {
            nb::list keys_to_remove;
            for (const auto& kv : delta_out) {
                nb::object key_obj = nb::cast<nb::object>(kv.first);
                nb::object value_obj = nb::cast<nb::object>(kv.second);
                if (is_empty_mapping_payload(value_obj)) {
                    keys_to_remove.append(key_obj);
                }
            }
            for (const auto& key_item : keys_to_remove) {
                nb::object key_obj = nb::cast<nb::object>(key_item);
                PyDict_DelItem(delta_out.ptr(), key_obj.ptr());
            }
        }

        if (PyDict_Size(delta_out.ptr()) > 0) {
            const value::TypeMeta* key_type = current->key_type();
            if (key_type != nullptr) {
                nb::object remove_marker = get_remove();
                nb::object remove_if_exists_marker = get_remove_if_exists();
                for (const auto& kv : delta_out) {
                    nb::object py_key = nb::cast<nb::object>(kv.first);
                    nb::object py_value = nb::cast<nb::object>(kv.second);
                    value::Value key_value(key_type);
                    key_value.emplace();
                    try {
                        key_type->ops().from_python(key_value.data(), py_key, key_type);
                    } catch (...) {
                        continue;
                    }
                    const View key_view = key_value.view();
                    if (py_value.is(remove_marker) || py_value.is(remove_if_exists_marker)) {
                        clear_tsd_visible_key_history(*data, key_view);
                    } else {
                        mark_tsd_visible_key_history(*data, key_view, current_time);
                    }
                }
            }
        }

        if (debug_tsd_delta) {
            std::string out_repr{"<repr_error>"};
            try {
                out_repr = nb::cast<std::string>(nb::repr(delta_out));
            } catch (...) {}
            std::fprintf(stderr,
                         "[tsd_delta_dbg] final_delta path=%s out=%s\n",
                         vd.path.to_string().c_str(),
                         out_repr.c_str());
        }
        return get_frozendict()(delta_out);
    }

    const auto sampled_delta_or_value = [&](const ViewData& child) -> nb::object {
        ViewData sampled_child = child;
        sampled_child.sampled = true;
        nb::object child_delta = op_delta_to_python(sampled_child, current_time);
        if (child_delta.is_none()) {
            View child_value = op_value(child);
            if (child_value.valid()) {
                child_delta = stored_delta_to_python(child_value);
            }
        }
        return child_delta;
    };

    const auto sampled_delta_or_python = [&](const ViewData& child) -> nb::object {
        ViewData sampled_child = child;
        sampled_child.sampled = true;
        nb::object child_delta = op_delta_to_python(sampled_child, current_time);
        if (child_delta.is_none()) {
            child_delta = op_to_python(child);
        }
        return child_delta;
    };

    if (current != nullptr && current->kind == TSKind::TSL) {
        ViewData current_bridge{};
        if (resolve_rebind_current_bridge_view(vd, self_meta, current_time, current_bridge)) {
            nb::dict delta_out;
            const size_t n = op_list_size(current_bridge);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = current_bridge;
                child.path.indices.push_back(i);
                if (!op_valid(child)) {
                    continue;
                }
                View child_value = op_value(child);
                if (child_value.valid()) {
                    delta_out[nb::int_(i)] = stored_delta_to_python(child_value);
                }
            }
            return delta_out;
        }

        nb::dict delta_out;
        const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
        const engine_time_t rebind_time = rebind_time_for_view(vd);
        const bool wrapper_ticked =
            wrapper_time == current_time ||
            rebind_time == current_time;
        const bool debug_tsl_delta = std::getenv("HGRAPH_DEBUG_TSL_DELTA") != nullptr;
        if (debug_tsl_delta) {
            int has_bound = 0;
            int bound_kind = -1;
            ViewData bound_dbg{};
            if (resolve_bound_target_view_data(vd, bound_dbg)) {
                has_bound = 1;
                if (const TSMeta* bm = meta_at_path(bound_dbg.meta, bound_dbg.path.indices); bm != nullptr) {
                    bound_kind = static_cast<int>(bm->kind);
                }
            }
            std::fprintf(stderr,
                         "[tsl_delta] path=%s now=%lld wrapper_ticked=%d wrapper_time=%lld rebind=%lld has_bound=%d bound_kind=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         wrapper_ticked ? 1 : 0,
                         static_cast<long long>(wrapper_time.time_since_epoch().count()),
                         static_cast<long long>(rebind_time.time_since_epoch().count()),
                         has_bound,
                         bound_kind);
        }

        bool sample_all = wrapper_ticked;
        if (sample_all) {
            const size_t n = op_list_size(*data);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                if (op_modified(child, current_time)) {
                    sample_all = false;
                    break;
                }
            }
        }

        if (sample_all) {
            const size_t n = op_list_size(*data);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                if (debug_tsl_delta) {
                    std::fprintf(stderr,
                                 "[tsl_delta]  sampled_child path=%s valid=%d modified=%d\n",
                                 child.path.to_string().c_str(),
                                 op_valid(child) ? 1 : 0,
                                 op_modified(child, current_time) ? 1 : 0);
                }
                if (!op_valid(child)) {
                    continue;
                }

                nb::object child_delta = sampled_delta_or_value(child);
                if (!child_delta.is_none()) {
                    delta_out[nb::int_(i)] = std::move(child_delta);
                }
            }
            return delta_out;
        }

        const size_t n = op_list_size(*data);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (debug_tsl_delta) {
                std::fprintf(stderr,
                             "[tsl_delta]  child path=%s valid=%d modified=%d\n",
                             child.path.to_string().c_str(),
                             op_valid(child) ? 1 : 0,
                             op_modified(child, current_time) ? 1 : 0);
            }
            if (!op_modified(child, current_time) || !op_valid(child)) {
                continue;
            }
            DeltaView child_delta = DeltaView::from_computed(child, current_time);
            if (child_delta.valid()) {
                delta_out[nb::int_(i)] = computed_delta_to_python(child_delta);
            }
        }
        return delta_out;
    }

    if (current != nullptr && current->kind == TSKind::TSB) {
        nb::dict delta_out;
        if (current->fields() == nullptr) {
            return delta_out;
        }

        ViewData current_bridge{};
        if (resolve_rebind_current_bridge_view(vd, self_meta, current_time, current_bridge)) {
            const TSMeta* bridge_meta = meta_at_path(current_bridge.meta, current_bridge.path.indices);
            if (bridge_meta != nullptr && bridge_meta->kind == TSKind::TSB && bridge_meta->fields() != nullptr) {
                for_each_named_bundle_field(bridge_meta, [&](size_t i, const char* field_name) {
                    ViewData child = current_bridge;
                    child.path.indices.push_back(i);
                    if (!op_valid(child)) {
                        return;
                    }

                    nb::object child_delta = sampled_delta_or_python(child);
                    if (child_delta.is_none()) {
                        return;
                    }
                    delta_out[nb::str(field_name)] = std::move(child_delta);
                });
                // Non-scalar delta contract: containers return empty payloads, not None.
                return delta_out;
            }
        }

        const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
        const engine_time_t rebind_time = rebind_time_for_view(vd);
        const bool wrapper_ticked =
            wrapper_time == current_time ||
            rebind_time == current_time;
        const bool debug_tsb_delta = std::getenv("HGRAPH_DEBUG_TSB_DELTA") != nullptr;
        bool suppress_wrapper_sampling = false;
        if (wrapper_ticked) {
            ViewData bound_target{};
            if (resolve_bound_target_view_data(vd, bound_target)) {
                const TSMeta* bound_meta = meta_at_path(bound_target.meta, bound_target.path.indices);
                suppress_wrapper_sampling = bound_meta != nullptr && bound_meta->kind == TSKind::REF;
            }
        }
        if (debug_tsb_delta) {
            int has_bound = 0;
            int bound_kind = -1;
            ViewData bound_dbg{};
            if (resolve_bound_target_view_data(vd, bound_dbg)) {
                has_bound = 1;
                if (const TSMeta* bm = meta_at_path(bound_dbg.meta, bound_dbg.path.indices); bm != nullptr) {
                    bound_kind = static_cast<int>(bm->kind);
                }
            }
            int has_prev = 0;
            int prev_kind = -1;
            int same_prev = -1;
            ViewData prev_dbg{};
            if (resolve_previous_bound_target_view_data(vd, prev_dbg)) {
                has_prev = 1;
                if (const TSMeta* pm = meta_at_path(prev_dbg.meta, prev_dbg.path.indices); pm != nullptr) {
                    prev_kind = static_cast<int>(pm->kind);
                }
                if (has_bound) {
                    same_prev =
                        prev_dbg.value_data == bound_dbg.value_data &&
                        prev_dbg.time_data == bound_dbg.time_data &&
                        prev_dbg.observer_data == bound_dbg.observer_data &&
                        prev_dbg.delta_data == bound_dbg.delta_data &&
                        prev_dbg.link_data == bound_dbg.link_data &&
                        prev_dbg.path.indices == bound_dbg.path.indices ? 1 : 0;
                }
            }
            std::fprintf(stderr,
                         "[tsb_delta] path=%s now=%lld uses_lt=%d wrapper_ticked=%d wrapper_time=%lld rebind=%lld suppress=%d has_bound=%d bound_kind=%d has_prev=%d prev_kind=%d same_prev=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         vd.uses_link_target ? 1 : 0,
                         wrapper_ticked ? 1 : 0,
                         static_cast<long long>(wrapper_time.time_since_epoch().count()),
                         static_cast<long long>(rebind_time.time_since_epoch().count()),
                         suppress_wrapper_sampling ? 1 : 0,
                         has_bound,
                         bound_kind,
                         has_prev,
                         prev_kind,
                         same_prev);
        }
        bool sample_all = wrapper_ticked && !suppress_wrapper_sampling;
        std::vector<bool> ref_item_rebound;
        if (wrapper_ticked && suppress_wrapper_sampling && current != nullptr) {
            ViewData bound_target{};
            if (resolve_bound_target_view_data(vd, bound_target)) {
                const TSMeta* bound_meta = meta_at_path(bound_target.meta, bound_target.path.indices);
                if (bound_meta != nullptr && bound_meta->kind == TSKind::REF) {
                    const TSMeta* element_meta = bound_meta->element_ts();
                    if (element_meta != nullptr && element_meta->kind == TSKind::TSB) {
                        ref_item_rebound.assign(current->field_count(), false);
                        const size_t n = std::min(current->field_count(), element_meta->field_count());
                        for (size_t i = 0; i < n; ++i) {
                            ViewData item = bound_target;
                            item.path.indices.push_back(i);
                            const engine_time_t item_lmt = direct_last_modified_time(item);
                            const engine_time_t item_op_lmt = op_last_modified_time(item);
                            const bool item_modified = op_modified(item, current_time);
                            ViewData resolved_item{};
                            const bool has_resolved_item = resolve_bound_target_view_data(item, resolved_item);
                            ViewData previous_item{};
                            const bool has_previous_item = resolve_previous_bound_target_view_data(item, previous_item);
                            ViewData previous_resolved_item{};
                            const bool has_previous_resolved_item =
                                has_resolved_item && resolve_previous_bound_target_view_data(resolved_item, previous_resolved_item);
                            const engine_time_t resolved_direct_lmt =
                                has_resolved_item ? direct_last_modified_time(resolved_item) : MIN_DT;
                            const bool resolved_item_modified =
                                has_resolved_item ? op_modified(resolved_item, current_time) : false;
                            const bool recorded_item_change = unbound_ref_item_changed_this_tick(item, i, current_time);
                            bool item_rebound = false;
                            if (has_resolved_item && has_previous_item) {
                                item_rebound = !is_same_view_data(resolved_item, previous_item);
                            } else if (has_resolved_item && has_previous_resolved_item) {
                                item_rebound = !is_same_view_data(resolved_item, previous_resolved_item);
                            }
                            ref_item_rebound[i] = item_rebound || resolved_item_modified || recorded_item_change;
                            if (debug_tsb_delta) {
                                std::fprintf(stderr,
                                             "[tsb_delta]  ref_item idx=%zu path=%s direct_lmt=%lld op_lmt=%lld now=%lld mod=%d resolved=%d resolved_path=%s prev=%d prev_path=%s prev_resolved=%d prev_resolved_path=%s resolved_direct_lmt=%lld resolved_mod=%d recorded=%d item_rebound=%d rebound=%d\n",
                                             i,
                                             item.path.to_string().c_str(),
                                             static_cast<long long>(item_lmt.time_since_epoch().count()),
                                             static_cast<long long>(item_op_lmt.time_since_epoch().count()),
                                             static_cast<long long>(current_time.time_since_epoch().count()),
                                             item_modified ? 1 : 0,
                                             has_resolved_item ? 1 : 0,
                                             has_resolved_item ? resolved_item.path.to_string().c_str() : "<none>",
                                             has_previous_item ? 1 : 0,
                                             has_previous_item ? previous_item.path.to_string().c_str() : "<none>",
                                             has_previous_resolved_item ? 1 : 0,
                                             has_previous_resolved_item ? previous_resolved_item.path.to_string().c_str() : "<none>",
                                             static_cast<long long>(resolved_direct_lmt.time_since_epoch().count()),
                                             resolved_item_modified ? 1 : 0,
                                             recorded_item_change ? 1 : 0,
                                             item_rebound ? 1 : 0,
                                             ref_item_rebound[i] ? 1 : 0);
                            }
                        }
                    }
                }
            }
        }
        const auto child_rebound_this_tick = [wrapper_ticked, &ref_item_rebound](size_t index, const ViewData& child) {
            if (!wrapper_ticked) {
                return false;
            }
            if (index < ref_item_rebound.size() && ref_item_rebound[index]) {
                return true;
            }
            ViewData previous{};
            if (!resolve_previous_bound_target_view_data(child, previous)) {
                return false;
            }
            ViewData current{};
            if (!resolve_bound_target_view_data(child, current)) {
                return false;
            }
            return !is_same_view_data(previous, current);
        };
        if (!sample_all && wrapper_ticked && suppress_wrapper_sampling && current != nullptr) {
            // On wrapper ticks sourced through REF[TSB], keep Python parity by
            // carrying unmodified non-scalar siblings when only scalar fields
            // advanced (for example switch branch changes).
            bool scalar_child_changed = false;
            bool has_unmodified_non_scalar_sibling = false;
            for (size_t i = 0; i < current->field_count(); ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                const bool scalar_like = child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind);
                const bool child_changed = child_rebound_this_tick(i, child) || op_modified(child, current_time);
                if (child_changed) {
                    if (scalar_like) {
                        scalar_child_changed = true;
                    } else {
                        scalar_child_changed = false;
                        has_unmodified_non_scalar_sibling = false;
                        break;
                    }
                } else if (!scalar_like && op_valid(child)) {
                    has_unmodified_non_scalar_sibling = true;
                }
            }
            if (scalar_child_changed && has_unmodified_non_scalar_sibling) {
                sample_all = true;
            }
        }
        if (sample_all) {
            // Only synthesize full wrapper snapshots when wrapper ticks require
            // carrying unmodified non-scalar siblings (for example switch/rebind).
            // For regular scalar-only sibling updates, emit normal per-child deltas.
            bool modified_scalar_like = false;
            bool modified_non_scalar = false;
            bool has_unmodified_non_scalar_sibling = false;
            for (size_t i = 0; i < current->field_count(); ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                const bool scalar_like =
                    child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind);
                const bool child_rebound = child_rebound_this_tick(i, child);
                const bool child_advanced =
                    op_last_modified_time(child) == current_time || child_rebound;
                if (debug_tsb_delta) {
                    const char* field_name = current->fields() != nullptr ? current->fields()[i].name : nullptr;
                    std::fprintf(stderr,
                                 "[tsb_delta]  probe field=%s path=%s scalar=%d rebound=%d advanced=%d valid=%d modified=%d lmt=%lld now=%lld\n",
                                 field_name != nullptr ? field_name : "<unnamed>",
                                 child.path.to_string().c_str(),
                                 scalar_like ? 1 : 0,
                                 child_rebound ? 1 : 0,
                                 child_advanced ? 1 : 0,
                                 op_valid(child) ? 1 : 0,
                                 op_modified(child, current_time) ? 1 : 0,
                                 static_cast<long long>(op_last_modified_time(child).time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                }
                if (child_advanced) {
                    if (scalar_like) {
                        modified_scalar_like = true;
                    } else {
                        modified_non_scalar = true;
                    }
                } else if (!scalar_like && op_valid(child)) {
                    has_unmodified_non_scalar_sibling = true;
                }
            }

            if (modified_non_scalar) {
                sample_all = false;
            } else if (modified_scalar_like && !has_unmodified_non_scalar_sibling) {
                sample_all = false;
            }
        }

        if (sample_all) {
            for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                if (!op_valid(child)) {
                    return;
                }

                nb::object child_delta = sampled_delta_or_value(child);
                if (!child_delta.is_none()) {
                    delta_out[nb::str(field_name)] = std::move(child_delta);
                }
            });
            // Non-scalar delta contract: containers return empty payloads, not None.
            return delta_out;
        }

        for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            const bool child_rebound = child_rebound_this_tick(i, child);
            if (debug_tsb_delta) {
                std::fprintf(stderr,
                             "[tsb_delta]  emit field=%s path=%s rebound=%d valid=%d modified=%d\n",
                             field_name,
                             child.path.to_string().c_str(),
                             child_rebound ? 1 : 0,
                             op_valid(child) ? 1 : 0,
                             op_modified(child, current_time) ? 1 : 0);
            }
            if ((!op_modified(child, current_time) && !child_rebound) || !op_valid(child)) {
                return;
            }

            const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
            if (child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind)) {
                DeltaView child_delta = DeltaView::from_computed(child, current_time);
                if (has_delta_payload(child_delta)) {
                    nb::object child_delta_py = computed_delta_to_python(child_delta);
                    if (!child_delta_py.is_none()) {
                        delta_out[nb::str(field_name)] = std::move(child_delta_py);
                    }
                } else if (child_rebound) {
                    View child_value = op_value(child);
                    if (child_value.valid()) {
                        nb::object child_value_py = stored_delta_to_python(child_value);
                        if (!child_value_py.is_none()) {
                            delta_out[nb::str(field_name)] = std::move(child_value_py);
                        }
                    }
                }
                return;
            }

            nb::object child_delta = op_delta_to_python(child, current_time);
            if (child_delta.is_none() && child_rebound) {
                child_delta = sampled_delta_or_python(child);
            }
            if (!child_delta.is_none()) {
                delta_out[nb::str(field_name)] = std::move(child_delta);
            }
        });
        // Non-scalar delta contract: containers return empty payloads, not None.
        return delta_out;
    }

    if (current != nullptr && current->kind == TSKind::TSValue) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        DeltaView delta = DeltaView::from_computed(vd, current_time);
        return computed_delta_to_python(delta);
    }

    DeltaView delta = DeltaView::from_computed(vd, current_time);
    return computed_delta_to_python(delta);
}



void prune_ref_unbound_item_change_state(RefUnboundItemChangeState& state, engine_time_t current_time) {
    if (current_time == MIN_DT) {
        return;
    }

    for (auto it = state.entries.begin(); it != state.entries.end();) {
        auto& records = it->second;
        records.erase(
            std::remove_if(
                records.begin(),
                records.end(),
                [current_time](const RefUnboundItemChangeRecord& record) {
                    return record.time < current_time;
                }),
            records.end());

        if (records.empty()) {
            it = state.entries.erase(it);
        } else {
            ++it;
        }
    }
}

std::shared_ptr<RefUnboundItemChangeState> ensure_ref_unbound_item_change_state(TSLinkObserverRegistry* registry) {
    if (registry == nullptr) {
        return {};
    }

    std::shared_ptr<void> existing = registry->feature_state(k_ref_unbound_item_change_state_key);
    if (existing) {
        return std::static_pointer_cast<RefUnboundItemChangeState>(std::move(existing));
    }

    auto state = std::make_shared<RefUnboundItemChangeState>();
    registry->set_feature_state(std::string{k_ref_unbound_item_change_state_key}, state);
    return state;
}

void record_unbound_ref_item_changes(const ViewData& source,
                                     const std::vector<size_t>& changed_indices,
                                     engine_time_t current_time) {
    if (changed_indices.empty() ||
        source.link_observer_registry == nullptr ||
        source.value_data == nullptr ||
        current_time == MIN_DT) {
        return;
    }

    auto state = ensure_ref_unbound_item_change_state(source.link_observer_registry);
    if (!state) {
        return;
    }
    prune_ref_unbound_item_change_state(*state, current_time);

    auto& records = state->entries[source.value_data];
    const auto existing = std::find_if(
        records.begin(),
        records.end(),
        [&source](const RefUnboundItemChangeRecord& record) {
            return record.path == source.path.indices;
        });

    if (existing != records.end()) {
        existing->time = current_time;
        existing->changed_indices = changed_indices;
        return;
    }

    RefUnboundItemChangeRecord record{};
    record.path = source.path.indices;
    record.time = current_time;
    record.changed_indices = changed_indices;
    records.push_back(std::move(record));
}

bool unbound_ref_item_changed_this_tick(const ViewData& item_view, size_t item_index, engine_time_t current_time) {
    if (item_view.path.indices.empty() ||
        item_view.link_observer_registry == nullptr ||
        item_view.value_data == nullptr ||
        current_time == MIN_DT) {
        return false;
    }

    std::shared_ptr<void> existing = item_view.link_observer_registry->feature_state(k_ref_unbound_item_change_state_key);
    if (!existing) {
        return false;
    }

    auto state = std::static_pointer_cast<RefUnboundItemChangeState>(std::move(existing));
    prune_ref_unbound_item_change_state(*state, current_time);
    const auto by_value = state->entries.find(item_view.value_data);
    if (by_value == state->entries.end()) {
        return false;
    }

    std::vector<size_t> parent_path = item_view.path.indices;
    parent_path.pop_back();
    const auto record = std::find_if(
        by_value->second.begin(),
        by_value->second.end(),
        [&parent_path, current_time](const RefUnboundItemChangeRecord& value) {
            return value.time == current_time && value.path == parent_path;
        });
    if (record == by_value->second.end()) {
        return false;
    }

    return std::find(record->changed_indices.begin(), record->changed_indices.end(), item_index) !=
           record->changed_indices.end();
}

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    const bool debug_ref_from = std::getenv("HGRAPH_DEBUG_REF_FROM") != nullptr;

    if (current != nullptr && current->kind == TSKind::REF) {
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
                (element_meta->kind == TSKind::TSS || element_meta->kind == TSKind::TSD);
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

    if (current != nullptr && current->kind == TSKind::TSW) {
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

        if (current->is_duration_based()) {
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

    if (current != nullptr && current->kind == TSKind::TSS) {
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

    if (current != nullptr && current->kind == TSKind::TSL) {
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

    if (current != nullptr && current->kind == TSKind::TSB) {
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

    if (current != nullptr && current->kind == TSKind::TSD) {
        if (reset_root_value_and_delta_on_none(vd, src, current_time)) {
            return;
        }

        auto maybe_dst = resolve_value_slot_mut(vd);
        if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_map()) {
            return;
        }

        const bool was_valid = op_valid(vd);
        auto dst_map = maybe_dst->as_map();
        auto slots = resolve_tsd_delta_slots(vd);
        clear_tsd_delta_if_new_tick(vd, current_time, slots);

        nb::object item_attr = nb::getattr(src, "items", nb::none());
        nb::iterator items = item_attr.is_none() ? nb::iter(src) : nb::iter(item_attr());

        const value::TypeMeta* key_type = current->key_type();
        const value::TypeMeta* value_type = current->element_ts() != nullptr ? current->element_ts()->value_type : nullptr;
        nb::object remove = get_remove();
        nb::object remove_if_exists = get_remove_if_exists();
        enum class RemoveMarkerKind {
            None,
            Remove,
            RemoveIfExists,
        };
        const auto classify_remove_marker = [&remove, &remove_if_exists](const nb::object& obj) -> RemoveMarkerKind {
            if (obj.is(remove)) {
                return RemoveMarkerKind::Remove;
            }
            if (obj.is(remove_if_exists)) {
                return RemoveMarkerKind::RemoveIfExists;
            }

            nb::object current = nb::getattr(obj, "name", nb::none());
            for (size_t depth = 0; depth < 4 && !current.is_none(); ++depth) {
                if (current.is(remove)) {
                    return RemoveMarkerKind::Remove;
                }
                if (current.is(remove_if_exists)) {
                    return RemoveMarkerKind::RemoveIfExists;
                }
                if (nb::isinstance<nb::str>(current)) {
                    std::string name = nb::cast<std::string>(current);
                    if (name == "REMOVE") {
                        return RemoveMarkerKind::Remove;
                    }
                    if (name == "REMOVE_IF_EXISTS") {
                        return RemoveMarkerKind::RemoveIfExists;
                    }
                    return RemoveMarkerKind::None;
                }
                current = nb::getattr(current, "name", nb::none());
            }

            return RemoveMarkerKind::None;
        };
        bool changed = false;

        for (const auto& kv : items) {
            value::Value key_value(key_type);
            key_value.emplace();
            key_type->ops().from_python(key_value.data(), kv[0], key_type);
            View key = key_value.view();

            nb::object value_obj = kv[1];
            if (value_obj.is_none()) {
                continue;
            }
            if (std::getenv("HGRAPH_DEBUG_TSD_FROM") != nullptr) {
                std::string key_s = nb::cast<std::string>(nb::repr(kv[0]));
                std::string val_s = nb::cast<std::string>(nb::repr(value_obj));
                const char* node_name = "<none>";
                if (vd.path.node != nullptr) {
                    node_name = vd.path.node->signature().name.c_str();
                }
                std::fprintf(stderr,
                             "[tsd_from] node=%s path=%s key=%s value=%s now=%lld\n",
                             node_name,
                             vd.path.to_string().c_str(),
                             key_s.c_str(),
                             val_s.c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()));
            }

            const RemoveMarkerKind marker = classify_remove_marker(value_obj);
            const bool is_remove = marker == RemoveMarkerKind::Remove;
            const bool is_remove_if_exists = marker == RemoveMarkerKind::RemoveIfExists;
            if (std::getenv("HGRAPH_DEBUG_TSD_FROM") != nullptr && (is_remove || is_remove_if_exists)) {
                const char* node_name = "<none>";
                if (vd.path.node != nullptr) {
                    node_name = vd.path.node->signature().name.c_str();
                }
                std::fprintf(stderr,
                             "[tsd_from] node=%s path=%s key=%s remove=%d remove_if_exists=%d\n",
                             node_name,
                             vd.path.to_string().c_str(),
                             key.to_string().c_str(),
                             is_remove ? 1 : 0,
                             is_remove_if_exists ? 1 : 0);
            }
            if (is_remove || is_remove_if_exists) {
                const auto removed_slot = map_slot_for_key(dst_map, key);
                const bool existed = removed_slot.has_value();
                if (!existed) {
                    const bool already_removed_this_tick =
                        slots.removed_set.valid() &&
                        slots.removed_set.is_set() &&
                        set_contains_key_relaxed(slots.removed_set.as_set(), key);
                    if (is_remove) {
                        if (already_removed_this_tick) {
                            // Idempotent same-tick replay: output nodes can re-apply their own
                            // emitted delta payload in the same evaluation pass.
                            continue;
                        }
                        throw nb::key_error("TSD key not found for REMOVE");
                    }
                    continue;
                }

                Value canonical_key_value = canonical_map_key_for_slot(dst_map, *removed_slot, key);
                const View canonical_key = canonical_key_value.view();
                bool removed_was_visible = false;
                ViewData child_vd = vd;
                child_vd.path.indices.push_back(*removed_slot);
                removed_was_visible = tsd_child_was_visible_before_removal(child_vd);
                record_tsd_removed_child_snapshot(vd, canonical_key, child_vd, current_time);

                bool added_this_cycle = false;
                if (slots.added_set.valid() && slots.added_set.is_set()) {
                    auto added = slots.added_set.as_set();
                    added_this_cycle = added.contains(canonical_key);
                    if (added_this_cycle) {
                        added.remove(canonical_key);
                    }
                }

                dst_map.remove(canonical_key);
                changed = true;
                compact_tsd_child_time_slot(vd, *removed_slot);
                compact_tsd_child_delta_slot(vd, *removed_slot);
                compact_tsd_child_link_slot(vd, *removed_slot);

                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    slots.changed_values_map.as_map().remove(canonical_key);
                }

                if (!added_this_cycle &&
                    slots.removed_set.valid() && slots.removed_set.is_set()) {
                    slots.removed_set.as_set().add(canonical_key);
                    if (!removed_was_visible &&
                        slots.added_set.valid() && slots.added_set.is_set()) {
                        // Marker: key-space changed without a visible value removal.
                        slots.added_set.as_set().add(canonical_key);
                    }
                }
                continue;
            }

            if (value_type == nullptr) {
                continue;
            }

            const TSMeta* element_meta = current->element_ts();
            const bool scalar_like_element = element_meta == nullptr || is_scalar_like_ts_kind(element_meta->kind);

            if (scalar_like_element) {
                if (nb::isinstance<TimeSeriesReference>(value_obj)) {
                    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(value_obj);
                    if (!ref.is_valid()) {
                        const bool existed = map_slot_for_key(dst_map, key).has_value();
                        if (!existed) {
                            // Python parity: tsd_get_items can emit empty references for keys
                            // that exist in source key-space. Materialize a placeholder keyed
                            // slot so later REMOVE/REMOVE_IF_EXISTS deltas can produce key-space
                            // ticks even if no concrete value was ever published.
                            value::Value blank_value(value_type);
                            blank_value.emplace();
                            dst_map.set(key, blank_value.view());

                            auto slot = map_slot_for_key(dst_map, key);
                            if (slot.has_value()) {
                                Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot, key);
                                const View canonical_key = canonical_key_value.view();
                                ensure_tsd_child_time_slot(vd, *slot);
                                ensure_tsd_child_delta_slot(vd, *slot);
                                ensure_tsd_child_link_slot(vd, *slot);
                                if (slots.added_set.valid() && slots.added_set.is_set()) {
                                    slots.added_set.as_set().add(canonical_key);
                                }
                                if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                                    slots.removed_set.as_set().remove(canonical_key);
                                }
                                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                                    slots.changed_values_map.as_map().remove(canonical_key);
                                }
                                changed = true;
                            }
                        }
                        continue;
                    }
                }

                value::Value value_value(value_type);
                value_value.emplace();
                value_type->ops().from_python(value_value.data(), value_obj, value_type);

                const bool existed = map_slot_for_key(dst_map, key).has_value();
                dst_map.set(key, value_value.view());
                changed = true;

                // Scalar-like TSD children do not recurse through op_from_python, so
                // stamp their child time path explicitly to keep child valid/modified
                // semantics aligned with Python runtime views.
                auto slot = map_slot_for_key(dst_map, key);
                if (slot.has_value()) {
                    Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot, key);
                    const View canonical_key = canonical_key_value.view();
                    ensure_tsd_child_time_slot(vd, *slot);
                    ensure_tsd_child_link_slot(vd, *slot);
                    ViewData child_vd = vd;
                    child_vd.path.indices.push_back(*slot);
                    stamp_time_paths(child_vd, current_time);
                    if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                        slots.changed_values_map.as_map().set(canonical_key, value_value.view());
                    }

                    if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
                        slots.added_set.as_set().add(canonical_key);
                    }

                    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                        slots.removed_set.as_set().remove(canonical_key);
                    }
                }
                continue;
            }

            const bool existed = map_slot_for_key(dst_map, key).has_value();
            if (!existed) {
                // Create a default child slot, then apply child delta/value recursively.
                value::Value blank_value(value_type);
                blank_value.emplace();
                dst_map.set(key, blank_value.view());
                if (slots.added_set.valid() && slots.added_set.is_set()) {
                    slots.added_set.as_set().add(key);
                }
            }

            auto slot = map_slot_for_key(dst_map, key);
            if (!slot.has_value()) {
                continue;
            }
            Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot, key);
            const View canonical_key = canonical_key_value.view();

            ensure_tsd_child_time_slot(vd, *slot);
            ensure_tsd_child_delta_slot(vd, *slot);
            ensure_tsd_child_link_slot(vd, *slot);
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(*slot);
            op_from_python(child_vd, value_obj, current_time);
            const bool child_changed = op_modified(child_vd, current_time);

            if (!child_changed && !existed) {
                // No-op nested updates (for example REMOVE_IF_EXISTS on a missing
                // child key) must not materialize an empty outer TSD entry.
                dst_map.remove(canonical_key);
                compact_tsd_child_time_slot(vd, *slot);
                compact_tsd_child_delta_slot(vd, *slot);
                compact_tsd_child_link_slot(vd, *slot);

                if (slots.added_set.valid() && slots.added_set.is_set()) {
                    auto added_set = slots.added_set.as_set();
                    added_set.remove(key);
                    added_set.remove(canonical_key);
                }
                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    slots.changed_values_map.as_map().remove(canonical_key);
                }
                if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                    slots.removed_set.as_set().remove(canonical_key);
                }
                continue;
            }

            if (child_changed) {
                changed = true;
                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    View child_value = op_value(child_vd);
                    if (child_value.valid()) {
                        slots.changed_values_map.as_map().set(canonical_key, child_value);
                    } else {
                        slots.changed_values_map.as_map().remove(canonical_key);
                    }
                }
            }

            if (child_changed && slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().remove(canonical_key);
            }
        }

        bool ref_child_target_modified = false;
        if (!changed &&
            current->element_ts() != nullptr &&
            current->element_ts()->kind == TSKind::REF) {
            auto current_value = resolve_value_slot_const(vd);
            if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
                for_each_map_key_slot(current_value->as_map(), [&](View /*key*/, size_t slot) {
                    if (ref_child_target_modified) {
                        return;
                    }
                    ViewData child_vd = vd;
                    child_vd.path.indices.push_back(slot);
                    ViewData target_vd{};
                    ref_child_target_modified =
                        resolve_bound_target_view_data(child_vd, target_vd) && op_modified(target_vd, current_time);
                });
            }
        }

        if (changed || !was_valid) {
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        } else if (ref_child_target_modified) {
            // Keep non-peer consumers of TSD[REF[...]] live when a compute node
            // returns an empty dict but referenced targets advanced.
            notify_link_target_observers(vd, current_time);
        }
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

