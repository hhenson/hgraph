#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view.h>

#include <exception>
#include <optional>

namespace hgraph {

enum class RefBindOrder {
    RefValueThenBoundTarget,
    BoundTargetThenRefValue,
};

inline bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
    return lhs.value_data == rhs.value_data &&
           lhs.time_data == rhs.time_data &&
           lhs.observer_data == rhs.observer_data &&
           lhs.delta_data == rhs.delta_data &&
           lhs.link_data == rhs.link_data &&
           lhs.link_observer_registry == rhs.link_observer_registry &&
           lhs.projection == rhs.projection &&
           lhs.path.indices == rhs.path.indices &&
           lhs.meta == rhs.meta;
}

inline bool resolve_ref_value_target_view_data(const TSView& ref_view, ViewData& out_target) {
    const TSMeta* meta = ref_view.ts_meta();
    if (meta == nullptr || meta->kind != TSKind::REF) {
        return false;
    }

    value::View payload = ref_view.value();
    if (!payload.valid()) {
        return false;
    }

    try {
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(payload.to_python());
        if (const ViewData* target = ref.bound_view();
            target != nullptr && !same_view_identity(*target, ref_view.view_data())) {
            out_target = *target;
            return true;
        }
    } catch (const std::exception&) {
        return false;
    }

    return false;
}

inline std::optional<ViewData> resolve_effective_view_data(const TSView& start_view,
                                                           RefBindOrder ref_bind_order = RefBindOrder::BoundTargetThenRefValue,
                                                           size_t max_depth = 8) {
    if (!start_view) {
        return std::nullopt;
    }

    ViewData cursor = start_view.view_data();
    const engine_time_t* current_time_ptr = start_view.view_data().engine_time_ptr;

    for (size_t depth = 0; depth < max_depth; ++depth) {
        const auto advance_to_bound_target = [&]() -> bool {
            ViewData bound_target{};
            if (!resolve_bound_target_view_data(cursor, bound_target) ||
                same_view_identity(bound_target, cursor)) {
                return false;
            }
            cursor = std::move(bound_target);
            return true;
        };

        const auto advance_to_ref_target = [&]() -> bool {
            ViewData ref_target{};
            if (!resolve_ref_value_target_view_data(TSView(cursor, current_time_ptr), ref_target)) {
                return false;
            }
            cursor = std::move(ref_target);
            return true;
        };

        bool advanced = false;
        if (ref_bind_order == RefBindOrder::BoundTargetThenRefValue) {
            advanced = advance_to_bound_target() || advance_to_ref_target();
        } else {
            advanced = advance_to_ref_target() || advance_to_bound_target();
        }

        if (!advanced) {
            break;
        }
    }

    return cursor;
}

inline TSView resolve_effective_view(const TSView& start_view,
                                     RefBindOrder ref_bind_order = RefBindOrder::BoundTargetThenRefValue,
                                     size_t max_depth = 8) {
    auto resolved = resolve_effective_view_data(start_view, ref_bind_order, max_depth);
    if (!resolved.has_value()) {
        return {};
    }
    return TSView(*resolved, start_view.view_data().engine_time_ptr);
}

inline std::optional<ViewData> resolve_non_ref_target_view_data(
    const TSView& start_view,
    RefBindOrder ref_bind_order = RefBindOrder::RefValueThenBoundTarget,
    size_t max_depth = 64) {
    auto resolved = resolve_effective_view_data(start_view, ref_bind_order, max_depth);
    if (!resolved.has_value()) {
        return std::nullopt;
    }

    TSView resolved_view(*resolved, start_view.view_data().engine_time_ptr);
    const TSMeta* resolved_meta = resolved_view.ts_meta();
    if (resolved_meta == nullptr || resolved_meta->kind == TSKind::REF) {
        return std::nullopt;
    }

    return resolved;
}

inline TSInputView node_input_field(Node& node, std::string_view name) {
    auto root = node.input();
    if (!root) {
        return {};
    }
    auto bundle_opt = root.try_as_bundle();
    if (!bundle_opt.has_value()) {
        return {};
    }
    return bundle_opt->field(name);
}

inline TSInputView node_inner_ts_input(Node& node, bool fallback_to_first = false) {
    auto root = node.input();
    if (!root) {
        return {};
    }

    auto bundle_opt = root.try_as_bundle();
    if (!bundle_opt.has_value()) {
        return {};
    }

    auto ts = bundle_opt->field("ts");
    if (!ts && fallback_to_first && bundle_opt->count() > 0) {
        ts = bundle_opt->at(0);
    }
    return ts;
}

inline TSView resolve_tsd_child_view(const TSInputView& tsd_input, const value::View& key) {
    if (!tsd_input || !key.valid()) {
        return {};
    }

    const engine_time_t* input_time_ptr = tsd_input.as_ts_view().view_data().engine_time_ptr;
    const auto normalize_child = [input_time_ptr](TSView child) -> TSView {
        if (!child) {
            return {};
        }
        if (child.valid()) {
            return child;
        }
        ViewData resolved_target{};
        if (resolve_bound_target_view_data(child.view_data(), resolved_target)) {
            return TSView(resolved_target, input_time_ptr);
        }
        return child;
    };

    auto tsd_opt = tsd_input.try_as_dict();
    if (!tsd_opt.has_value()) {
        return {};
    }

    TSView direct_child = normalize_child(tsd_opt->as_ts_view().as_dict().at_key(key));
    if (direct_child && direct_child.valid()) {
        return direct_child;
    }

    ViewData bound_target{};
    if (resolve_bound_target_view_data(tsd_opt->as_ts_view().view_data(), bound_target)) {
        TSView bound_child = normalize_child(TSView(bound_target, input_time_ptr).child_by_key(key));
        if (bound_child && bound_child.valid()) {
            return bound_child;
        }
        if (bound_child) {
            return bound_child;
        }
    }

    return direct_child;
}

inline void bind_inner_from_outer(const TSView& outer_any,
                                  TSInputView inner_any,
                                  RefBindOrder ref_bind_order = RefBindOrder::RefValueThenBoundTarget) {
    if (!inner_any) {
        return;
    }

    if (!outer_any) {
        inner_any.unbind();
        return;
    }

    const engine_time_t* inner_time_ptr = inner_any.as_ts_view().view_data().engine_time_ptr;

    const auto bind_from_outer = [&]() {
        inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_time_ptr));
    };

    const auto bind_bound_target_if_present = [&]() -> bool {
        ViewData bound_target{};
        if (!resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
            return false;
        }
        inner_any.as_ts_view().bind(TSView(bound_target, inner_time_ptr));
        return true;
    };

    const auto bind_ref_value_if_present = [&]() -> bool {
        value::View ref_view = outer_any.value();
        if (!ref_view.valid()) {
            return false;
        }
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_view.to_python());
        ref.bind_input(inner_any);
        return true;
    };

    const TSMeta* outer_meta = outer_any.ts_meta();
    if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
        if (ref_bind_order == RefBindOrder::BoundTargetThenRefValue) {
            if (bind_bound_target_if_present()) {
                return;
            }
            if (bind_ref_value_if_present()) {
                return;
            }
        } else {
            if (bind_ref_value_if_present()) {
                return;
            }
            if (bind_bound_target_if_present()) {
                return;
            }
        }

        bind_from_outer();
        return;
    }

    if (!bind_bound_target_if_present()) {
        bind_from_outer();
    }
}

}  // namespace hgraph
