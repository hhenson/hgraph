/**
 * @file ts_view.cpp
 * @brief Implementation of TSView - Non-owning time-series view.
 */

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/fq_path.h>
#include <hgraph/types/node.h>
#include <hgraph/types/value/map_storage.h>

#include <stdexcept>

namespace hgraph {

// ============================================================================
// TSView Construction
// ============================================================================

TSView::TSView(TSValue& ts_value, engine_time_t current_time)
    : view_data_(ts_value.make_view_data())
    , current_time_(current_time)
{
}

// ============================================================================
// Time-Series Semantics (dispatch through ts_ops)
// ============================================================================

engine_time_t TSView::last_modified_time() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return MIN_ST;
    }
    return view_data_.ops->last_modified_time(view_data_);
}

bool TSView::modified() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return false;
    }
    return view_data_.ops->modified(view_data_, current_time_);
}

bool TSView::valid() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return false;
    }
    return view_data_.ops->valid(view_data_);
}

bool TSView::all_valid() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return false;
    }
    return view_data_.ops->all_valid(view_data_);
}

bool TSView::sampled() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return false;
    }
    return view_data_.ops->sampled(view_data_);
}

bool TSView::has_delta() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return false;
    }
    return view_data_.ops->has_delta(view_data_);
}

// ============================================================================
// Value Access
// ============================================================================

value::View TSView::value() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return value::View{};
    }
    return view_data_.ops->value(view_data_);
}

value::View TSView::delta_value() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return value::View{};
    }
    return view_data_.ops->delta_value(view_data_);
}

// ============================================================================
// Mutation (for outputs)
// ============================================================================

void TSView::set_value(const value::View& src) {
    if (!view_data_.valid() || !view_data_.ops) {
        throw std::runtime_error("set_value on invalid TSView");
    }
    view_data_.ops->set_value(view_data_, src, current_time_);
}

void TSView::apply_delta(const value::View& delta) {
    if (!view_data_.valid() || !view_data_.ops) {
        throw std::runtime_error("apply_delta on invalid TSView");
    }
    view_data_.ops->apply_delta(view_data_, delta, current_time_);
}

void TSView::invalidate() {
    if (!view_data_.valid() || !view_data_.ops) {
        throw std::runtime_error("invalidate on invalid TSView");
    }
    view_data_.ops->invalidate(view_data_);
}

// ============================================================================
// Python Interop
// ============================================================================

nb::object TSView::to_python() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return nb::none();
    }
    return view_data_.ops->to_python(view_data_);
}

nb::object TSView::delta_to_python() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return nb::none();
    }
    return view_data_.ops->delta_to_python(view_data_);
}

void TSView::from_python(const nb::object& src) {
    if (!view_data_.valid() || !view_data_.ops) {
        throw std::runtime_error("from_python on invalid TSView");
    }
    view_data_.ops->from_python(view_data_, src, current_time_);
}

// ============================================================================
// Navigation
// ============================================================================

TSView TSView::operator[](size_t index) const {
    if (!view_data_.valid() || !view_data_.ops) {
        return TSView{};
    }
    return view_data_.ops->child_at(view_data_, index, current_time_);
}

TSView TSView::field(const std::string& name) const {
    if (!view_data_.valid() || !view_data_.ops) {
        return TSView{};
    }
    return view_data_.ops->child_by_name(view_data_, name, current_time_);
}

size_t TSView::size() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return 0;
    }
    return view_data_.ops->child_count(view_data_);
}

// ============================================================================
// Observer Access
// ============================================================================

value::View TSView::observer() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return value::View{};
    }
    return view_data_.ops->observer(view_data_);
}

// ============================================================================
// Binding (Link Management)
// ============================================================================

void TSView::bind(const TSView& target) {
    if (!view_data_.valid() || !view_data_.ops) {
        throw std::runtime_error("bind on invalid TSView");
    }
    view_data_.ops->bind(view_data_, target.view_data_);
}

void TSView::unbind() {
    if (!view_data_.valid() || !view_data_.ops) {
        throw std::runtime_error("unbind on invalid TSView");
    }
    view_data_.ops->unbind(view_data_);
}

bool TSView::is_bound() const {
    if (!view_data_.valid() || !view_data_.ops) {
        return false;
    }
    return view_data_.ops->is_bound(view_data_);
}

// ============================================================================
// Kind-Specific View Conversions
// ============================================================================

TSBView TSView::as_bundle() const {
    if (!view_data_.valid() || !view_data_.meta) {
        throw std::runtime_error("as_bundle on invalid TSView");
    }
    if (view_data_.meta->kind != TSKind::TSB) {
        throw std::runtime_error("as_bundle called on non-TSB type");
    }
    return TSBView(view_data_, current_time_);
}

TSLView TSView::as_list() const {
    if (!view_data_.valid() || !view_data_.meta) {
        throw std::runtime_error("as_list on invalid TSView");
    }
    if (view_data_.meta->kind != TSKind::TSL) {
        throw std::runtime_error("as_list called on non-TSL type");
    }
    return TSLView(view_data_, current_time_);
}

TSSView TSView::as_set() const {
    if (!view_data_.valid() || !view_data_.meta) {
        throw std::runtime_error("as_set on invalid TSView");
    }
    if (view_data_.meta->kind != TSKind::TSS) {
        throw std::runtime_error("as_set called on non-TSS type");
    }
    return TSSView(view_data_, current_time_);
}

TSDView TSView::as_dict() const {
    if (!view_data_.valid() || !view_data_.meta) {
        throw std::runtime_error("as_dict on invalid TSView");
    }
    if (view_data_.meta->kind != TSKind::TSD) {
        throw std::runtime_error("as_dict called on non-TSD type");
    }
    return TSDView(view_data_, current_time_);
}

TSWView TSView::as_window() const {
    if (!view_data_.valid() || !view_data_.meta) {
        throw std::runtime_error("as_window on invalid TSView");
    }
    if (view_data_.meta->kind != TSKind::TSW) {
        throw std::runtime_error("as_window called on non-TSW type");
    }
    return TSWView(view_data_, current_time_);
}

// ============================================================================
// FQPath Access
// ============================================================================

FQPath TSView::fq_path() const {
    // TSView without owner context can only provide indices (not full semantic conversion)
    // For full FQPath with field names and keys, use TSOutputView::fq_path() or TSInputView::fq_path()
    const ShortPath& sp = view_data_.path;

    std::vector<int64_t> node_id;
    if (sp.node()) {
        node_id = sp.node()->node_id();
    }

    FQPath fq(node_id, sp.port_type());

    // Add indices as PathElement::index (since we don't have root context for semantic conversion)
    for (size_t idx : sp.indices()) {
        fq.push_index(idx);
    }

    return fq;
}

// ============================================================================
// ShortPath
// ============================================================================

std::string ShortPath::to_string() const {
    std::string result;

    if (node_) {
        result += "node[";
        result += std::to_string(node_->node_ndx());
        result += "]";
    } else {
        result += "<invalid>";
    }

    result += (port_type_ == PortType::INPUT ? ".in" : ".out");

    for (size_t idx : indices_) {
        result += "[";
        result += std::to_string(idx);
        result += "]";
    }

    return result;
}

FQPath ShortPath::to_fq(const ViewData& root_vd) const {
    if (!node_) {
        throw std::runtime_error("ShortPath::to_fq() called on invalid path (no node)");
    }

    // Start with node ID and port type
    FQPath fq(node_->node_id(), port_type_);

    if (indices_.empty()) {
        return fq;  // Root path, nothing to navigate
    }

    // Navigate through the ViewData, extracting semantic path elements
    ViewData current_vd = root_vd;

    for (size_t idx : indices_) {
        if (!current_vd.valid() || !current_vd.meta) {
            throw std::runtime_error("ShortPath::to_fq() navigation failed: invalid ViewData");
        }

        switch (current_vd.meta->kind) {
            case TSKind::TSB: {
                // Bundle: convert index to field name
                if (idx >= current_vd.meta->field_count) {
                    throw std::runtime_error("ShortPath::to_fq() bundle index out of range");
                }
                const TSBFieldInfo& field_info = current_vd.meta->fields[idx];
                fq.push_field(field_info.name);

                // Navigate to child
                current_vd = current_vd.child_at(idx);
                break;
            }

            case TSKind::TSL: {
                // List: index stays as index
                fq.push_index(idx);

                // Navigate to child
                current_vd = current_vd.child_at(idx);
                break;
            }

            case TSKind::TSD: {
                // Dict: convert slot index to actual key value
                auto* storage = static_cast<value::MapStorage*>(current_vd.value_data);
                if (!storage) {
                    throw std::runtime_error("ShortPath::to_fq() TSD has no storage");
                }

                if (!storage->key_set().is_alive(idx)) {
                    throw std::runtime_error("ShortPath::to_fq() TSD slot is not alive");
                }

                // Get the key from the slot
                const void* key_ptr = storage->key_at_slot(idx);
                const value::TypeMeta* key_type = storage->key_type();

                // Create a View from the key and clone it to PathElement
                value::View key_view(key_ptr, key_type);
                fq.push(PathElement::key_from_view(key_view));

                // Navigate to child
                current_vd = current_vd.child_at(idx);
                break;
            }

            default:
                // Scalar types shouldn't have children to navigate
                throw std::runtime_error("ShortPath::to_fq() unexpected navigation into scalar type");
        }
    }

    return fq;
}

TSView ShortPath::resolve(engine_time_t current_time) const {
    if (!node_) {
        throw std::runtime_error("ShortPath::resolve() called on invalid path (no node)");
    }

    TSView view;

    if (port_type_ == PortType::INPUT) {
        TSInput* input = node_->ts_input();
        if (!input) {
            throw std::runtime_error("ShortPath::resolve() failed: node has no TSInput");
        }
        view = input->value().ts_view(current_time);
    } else {
        TSOutput* output = node_->ts_output();
        if (!output) {
            throw std::runtime_error("ShortPath::resolve() failed: node has no TSOutput");
        }
        view = output->native_value().ts_view(current_time);
    }

    // Navigate through indices
    for (size_t idx : indices_) {
        view = view[idx];
    }

    // Set the path on the resolved view
    view.view_data().path = *this;

    return view;
}

// ============================================================================
// ViewData
// ============================================================================

ViewData ViewData::child_at(size_t index) const {
    if (!valid() || !ops) {
        return ViewData{};
    }

    // Delegate to ts_ops::child_at and extract the ViewData
    // Use MIN_ST as the time since ViewData doesn't track current_time
    TSView child = ops->child_at(*this, index, MIN_ST);
    return child.view_data();
}

ViewData ViewData::child_by_name(const std::string& name) const {
    if (!valid() || !ops) {
        return ViewData{};
    }

    // Delegate to ts_ops::child_by_name and extract the ViewData
    TSView child = ops->child_by_name(*this, name, MIN_ST);
    return child.view_data();
}

// ============================================================================
// TSValue::make_view_data Implementation
// ============================================================================

ViewData TSValue::make_view_data() {
    return ViewData{
        ShortPath{},  // Path will be set by caller
        value_.view().data(),
        time_.view().data(),
        observer_.view().data(),
        delta_value_.valid() ? delta_value_.view().data() : nullptr,
        link_.valid() ? link_.view().data() : nullptr,
        get_ts_ops(meta_),
        meta_
    };
}

TSView TSValue::ts_view(engine_time_t current_time) {
    return TSView(*this, current_time);
}

} // namespace hgraph
