#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view.h>

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
