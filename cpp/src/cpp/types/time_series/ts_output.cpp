#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/node.h>

namespace hgraph {
namespace {

size_t find_field_index(const TSMeta* bundle_meta, std::string_view field_name) {
    if (bundle_meta == nullptr || bundle_meta->fields() == nullptr) {
        return static_cast<size_t>(-1);
    }

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        const char* name = bundle_meta->fields()[i].name;
        if (name != nullptr && field_name == name) {
            return i;
        }
    }
    return static_cast<size_t>(-1);
}

void establish_links_recursive(TSView native_view, const TSMeta* native_meta,
                               TSView alt_view, const TSMeta* alt_meta) {
    if (!native_view || !alt_view || native_meta == nullptr || alt_meta == nullptr) {
        return;
    }

    const bool fixed_tsl_pair =
        native_meta->kind == TSKind::TSL &&
        alt_meta->kind == TSKind::TSL &&
        native_meta->fixed_size() > 0 &&
        native_meta->fixed_size() == alt_meta->fixed_size();

    // Fixed-size TSL alternatives bind children only (container slot remains unlinked).
    if (!fixed_tsl_pair) {
        alt_view.bind(native_view);
    }

    if (native_meta == alt_meta) {
        return;
    }

    if (native_meta->kind == TSKind::REF || alt_meta->kind == TSKind::REF) {
        return;
    }

    if (native_meta->kind == TSKind::TSB && alt_meta->kind == TSKind::TSB) {
        if (native_meta->fields() == nullptr || alt_meta->fields() == nullptr) {
            return;
        }

        for (size_t alt_index = 0; alt_index < alt_meta->field_count(); ++alt_index) {
            const char* alt_name = alt_meta->fields()[alt_index].name;
            size_t native_index = alt_index;
            if (alt_name != nullptr) {
                const size_t by_name = find_field_index(native_meta, alt_name);
                if (by_name != static_cast<size_t>(-1)) {
                    native_index = by_name;
                }
            }

            if (native_index >= native_meta->field_count()) {
                continue;
            }

            establish_links_recursive(
                native_view.child_at(native_index),
                native_meta->fields()[native_index].ts_type,
                alt_view.child_at(alt_index),
                alt_meta->fields()[alt_index].ts_type);
        }
        return;
    }

    if (fixed_tsl_pair) {
        const size_t n = native_meta->fixed_size();
        for (size_t i = 0; i < n; ++i) {
            establish_links_recursive(
                native_view.child_at(i),
                native_meta->element_ts(),
                alt_view.child_at(i),
                alt_meta->element_ts());
        }
    }
}

}  // namespace

TSOutput::TSOutput(const TSMeta* meta, node_ptr owning_node, size_t port_index)
    : link_observer_registry_(std::make_shared<TSLinkObserverRegistry>()),
      native_value_(meta),
      owning_node_(owning_node),
      port_index_(port_index) {
    native_value_.set_link_observer_registry(link_observer_registry_.get());
}

const engine_time_t* TSOutput::owner_engine_time_ptr() const noexcept {
    if (owning_node_ == nullptr) {
        return nullptr;
    }
    return owning_node_->cached_evaluation_time_ptr();
}

TSView TSOutput::view() {
    return TSView(native_value_, owner_engine_time_ptr(), runtime_root_path());
}

TSView TSOutput::view_for_schema(const TSMeta* schema) {
    const engine_time_t* engine_time_ptr = owner_engine_time_ptr();
    if (schema == nullptr || schema == native_value_.meta()) {
        return view();
    }

    TSValue& alt = get_or_create_alternative(schema);
    return TSView(alt, engine_time_ptr, runtime_root_path());
}

TSView TSOutput::view_for_input(const TSInput& input) {
    const TSMeta* input_meta = input.meta();
    if (input_meta == nullptr) {
        return view();
    }
    return view_for_schema(input_meta);
}

TSOutputView TSOutput::output_view() {
    return TSOutputView(this, view());
}

TSOutputView TSOutput::output_view_for_input(const TSInput& input) {
    return TSOutputView(this, view_for_input(input));
}

TSValue& TSOutput::get_or_create_alternative(const TSMeta* schema) {
    auto it = alternatives_.find(schema);
    if (it != alternatives_.end()) {
        return it->second;
    }

    auto [inserted, _] = alternatives_.emplace(schema, TSValue(schema));
    inserted->second.set_link_observer_registry(link_observer_registry_.get());
    establish_default_binding(inserted->second);
    return inserted->second;
}

void TSOutput::establish_default_binding(TSValue& alternative) {
    TSView target_view(native_value_, static_cast<const engine_time_t*>(nullptr), runtime_root_path());
    TSView alt_view(alternative, static_cast<const engine_time_t*>(nullptr), runtime_root_path());

    establish_links_recursive(target_view, native_value_.meta(), alt_view, alternative.meta());
}

}  // namespace hgraph
