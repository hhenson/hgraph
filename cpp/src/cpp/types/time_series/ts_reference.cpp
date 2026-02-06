#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ts_reference_ops.h>
#include <hgraph/types/time_series/ts_view.h>

#include <nanobind/nanobind.h>

#include <iostream>
#include <sstream>
#include <new>

namespace nb = nanobind;

namespace hgraph {

// ============================================================================
// TSReference - Lifetime Management
// ============================================================================

void TSReference::destroy() noexcept {
    switch (kind_) {
        case Kind::EMPTY:
            // Nothing to destroy
            break;
        case Kind::PEERED:
            storage_.peered_path.~ShortPath();
            break;
        case Kind::NON_PEERED:
            storage_.non_peered_items.~vector();
            break;
    }
    kind_ = Kind::EMPTY;
}

void TSReference::copy_from(const TSReference& other) {
    kind_ = other.kind_;
    switch (kind_) {
        case Kind::EMPTY:
            break;
        case Kind::PEERED:
            new (&storage_.peered_path) ShortPath(other.storage_.peered_path);
            break;
        case Kind::NON_PEERED:
            new (&storage_.non_peered_items) std::vector<TSReference>(other.storage_.non_peered_items);
            break;
    }
}

void TSReference::move_from(TSReference&& other) noexcept {
    kind_ = other.kind_;
    switch (kind_) {
        case Kind::EMPTY:
            break;
        case Kind::PEERED:
            new (&storage_.peered_path) ShortPath(std::move(other.storage_.peered_path));
            break;
        case Kind::NON_PEERED:
            new (&storage_.non_peered_items) std::vector<TSReference>(std::move(other.storage_.non_peered_items));
            break;
    }
    other.destroy();
}

// ============================================================================
// TSReference - Construction / Assignment / Destruction
// ============================================================================

TSReference::TSReference(const TSReference& other) : kind_(Kind::EMPTY) {
    copy_from(other);
}

TSReference::TSReference(TSReference&& other) noexcept : kind_(Kind::EMPTY) {
    move_from(std::move(other));
}

TSReference& TSReference::operator=(const TSReference& other) {
    if (this != &other) {
        destroy();
        copy_from(other);
    }
    return *this;
}

TSReference& TSReference::operator=(TSReference&& other) noexcept {
    if (this != &other) {
        destroy();
        move_from(std::move(other));
    }
    return *this;
}

TSReference::~TSReference() {
    destroy();
}

// ============================================================================
// TSReference - Factory Methods
// ============================================================================

TSReference TSReference::peered(ShortPath path) {
    TSReference ref;
    ref.kind_ = Kind::PEERED;
    new (&ref.storage_.peered_path) ShortPath(std::move(path));
    return ref;
}

TSReference TSReference::non_peered(std::vector<TSReference> items) {
    TSReference ref;
    ref.kind_ = Kind::NON_PEERED;
    new (&ref.storage_.non_peered_items) std::vector<TSReference>(std::move(items));
    return ref;
}

// ============================================================================
// TSReference - Query Methods
// ============================================================================

bool TSReference::is_valid(engine_time_t current_time) const {
    switch (kind_) {
        case Kind::EMPTY:
            return false;
        case Kind::PEERED: {
            if (!storage_.peered_path.valid()) {
                return false;
            }
            // Try to resolve and check validity
            try {
                TSView view = storage_.peered_path.resolve(current_time);
                return view.valid();
            } catch (...) {
                return false;
            }
        }
        case Kind::NON_PEERED:
            // Valid if any item is non-empty
            for (const auto& item : storage_.non_peered_items) {
                if (!item.is_empty()) {
                    return true;
                }
            }
            return false;
    }
    return false;
}

// ============================================================================
// TSReference - Accessors
// ============================================================================

const ShortPath& TSReference::path() const {
    if (kind_ != Kind::PEERED) {
        throw std::runtime_error("TSReference::path() called on non-PEERED reference");
    }
    return storage_.peered_path;
}

const std::vector<TSReference>& TSReference::items() const {
    if (kind_ != Kind::NON_PEERED) {
        throw std::runtime_error("TSReference::items() called on non-NON_PEERED reference");
    }
    return storage_.non_peered_items;
}

const TSReference& TSReference::operator[](size_t index) const {
    if (kind_ != Kind::NON_PEERED) {
        throw std::runtime_error("TSReference::operator[] called on non-NON_PEERED reference");
    }
    if (index >= storage_.non_peered_items.size()) {
        throw std::out_of_range("TSReference::operator[] index out of range");
    }
    return storage_.non_peered_items[index];
}

size_t TSReference::size() const noexcept {
    if (kind_ != Kind::NON_PEERED) {
        return 0;
    }
    return storage_.non_peered_items.size();
}

// ============================================================================
// TSReference - Resolution
// ============================================================================

TSView TSReference::resolve(engine_time_t current_time) const {
    if (kind_ != Kind::PEERED) {
        throw std::runtime_error("TSReference::resolve() called on non-PEERED reference");
    }
    return storage_.peered_path.resolve(current_time);
}

// ============================================================================
// TSReference - Conversion
// ============================================================================

FQReference TSReference::to_fq() const {
    switch (kind_) {
        case Kind::EMPTY:
            return FQReference::empty();

        case Kind::PEERED: {
            const auto& sp = storage_.peered_path;
            // Get node_id from node pointer
            // Note: This requires Node to have a method to get its ID
            int node_id = -1;
            if (sp.node()) {
                // TODO: Implement node->node_id() accessor
                // For now, we'll need to add this to Node interface
                // node_id = sp.node()->node_id();
                node_id = 0;  // Placeholder
            }
            return FQReference::peered(node_id, sp.port_type(), sp.indices());
        }

        case Kind::NON_PEERED: {
            std::vector<FQReference> fq_items;
            fq_items.reserve(storage_.non_peered_items.size());
            for (const auto& item : storage_.non_peered_items) {
                fq_items.push_back(item.to_fq());
            }
            return FQReference::non_peered(std::move(fq_items));
        }
    }
    return FQReference::empty();
}

TSReference TSReference::from_fq(const FQReference& fq, Graph* graph) {
    switch (fq.kind) {
        case Kind::EMPTY:
            return TSReference::empty();

        case Kind::PEERED: {
            // Resolve node_id to Node*
            // TODO: Implement graph->get_node(node_id)
            node_ptr node = nullptr;
            if (graph && fq.node_id >= 0) {
                // node = graph->get_node(fq.node_id);
            }
            ShortPath path(node, fq.port_type, fq.indices);
            return TSReference::peered(std::move(path));
        }

        case Kind::NON_PEERED: {
            std::vector<TSReference> items;
            items.reserve(fq.items.size());
            for (const auto& fq_item : fq.items) {
                items.push_back(TSReference::from_fq(fq_item, graph));
            }
            return TSReference::non_peered(std::move(items));
        }
    }
    return TSReference::empty();
}

// ============================================================================
// TSReference - Comparison
// ============================================================================

bool TSReference::operator==(const TSReference& other) const {
    if (kind_ != other.kind_) {
        return false;
    }
    switch (kind_) {
        case Kind::EMPTY:
            return true;
        case Kind::PEERED:
            return storage_.peered_path == other.storage_.peered_path;
        case Kind::NON_PEERED:
            return storage_.non_peered_items == other.storage_.non_peered_items;
    }
    return false;
}

// ============================================================================
// TSReference - String Representation
// ============================================================================

std::string TSReference::to_string() const {
    switch (kind_) {
        case Kind::EMPTY:
            return "REF[<Empty>]";

        case Kind::PEERED:
            return "REF[" + storage_.peered_path.to_string() + "]";

        case Kind::NON_PEERED: {
            std::ostringstream oss;
            oss << "REF[";
            bool first = true;
            for (const auto& item : storage_.non_peered_items) {
                if (!first) oss << ", ";
                first = false;
                // Recursively get string, but strip outer "REF[...]"
                std::string item_str = item.to_string();
                if (item_str.size() > 5 && item_str.substr(0, 4) == "REF[") {
                    oss << item_str.substr(4, item_str.size() - 5);
                } else {
                    oss << item_str;
                }
            }
            oss << "]";
            return oss.str();
        }
    }
    return "REF[<Unknown>]";
}

// ============================================================================
// FQReference - String Representation
// ============================================================================

std::string FQReference::to_string() const {
    switch (kind) {
        case Kind::EMPTY:
            return "FQRef[<Empty>]";

        case Kind::PEERED: {
            std::ostringstream oss;
            oss << "FQRef[node=" << node_id
                << ", port=" << (port_type == PortType::INPUT ? "IN" : "OUT");
            if (!indices.empty()) {
                oss << ", path=[";
                bool first = true;
                for (size_t idx : indices) {
                    if (!first) oss << ",";
                    first = false;
                    oss << idx;
                }
                oss << "]";
            }
            oss << "]";
            return oss.str();
        }

        case Kind::NON_PEERED: {
            std::ostringstream oss;
            oss << "FQRef[";
            bool first = true;
            for (const auto& item : items) {
                if (!first) oss << ", ";
                first = false;
                oss << item.to_string();
            }
            oss << "]";
            return oss.str();
        }
    }
    return "FQRef[<Unknown>]";
}

} // namespace hgraph

// ============================================================================
// ScalarOps<TSReference> - Python Interop Implementation
// ============================================================================

namespace hgraph::value {

nb::object ScalarOps<TSReference>::to_python(const void* obj, const TypeMeta*) {
    const auto& ref = *static_cast<const TSReference*>(obj);

    // Import Python TimeSeriesReference module
    auto ref_module = nb::module_::import_("hgraph._types._ref_type");

    switch (ref.kind()) {
        case TSReference::Kind::EMPTY: {
            // Return EmptyTimeSeriesReference via TimeSeriesReference.make()
            auto make_fn = ref_module.attr("TimeSeriesReference").attr("make");
            return make_fn();  // No arguments = empty reference
        }

        case TSReference::Kind::PEERED: {
            // For PEERED, we need to resolve the path to an actual output
            // This is complex because we need runtime context
            // For now, we return a placeholder that can be reconstructed
            // TODO: Proper resolution requires current_time and graph context

            // Create FQReference for transfer
            FQReference fq = ref.to_fq();

            // Return as a dict that Python can interpret
            nb::dict result;
            result["kind"] = "PEERED";
            result["node_id"] = fq.node_id;
            result["port_type"] = fq.port_type == PortType::INPUT ? "INPUT" : "OUTPUT";

            nb::list indices_list;
            for (size_t idx : fq.indices) {
                indices_list.append(idx);
            }
            result["indices"] = indices_list;

            // For now, wrap in a marker class or just return the dict
            // The Python side will need to handle this appropriately
            return result;
        }

        case TSReference::Kind::NON_PEERED: {
            // Convert items recursively
            nb::list items_list;
            for (const auto& item : ref.items()) {
                items_list.append(to_python(&item, nullptr));
            }

            // Create UnBoundTimeSeriesReference via TimeSeriesReference.make(from_items=...)
            auto make_fn = ref_module.attr("TimeSeriesReference").attr("make");
            return make_fn(nb::arg("from_items") = items_list);
        }
    }

    return nb::none();
}

void ScalarOps<TSReference>::from_python(void* dst, const nb::object& src, const TypeMeta*) {
    auto& ref = *static_cast<TSReference*>(dst);

    if (src.is_none()) {
        ref = TSReference::empty();
        return;
    }

    // Check if it's a C++ TSReference directly
    if (nb::isinstance<TSReference>(src)) {
        ref = nb::cast<TSReference>(src);
        return;
    }

    // Check if it's a dict (from our to_python for PEERED)
    if (nb::isinstance<nb::dict>(src)) {
        auto dict = nb::cast<nb::dict>(src);
        if (dict.contains("kind")) {
            std::string kind = nb::cast<std::string>(dict["kind"]);
            if (kind == "PEERED") {
                int node_id = nb::cast<int>(dict["node_id"]);
                std::string port_str = nb::cast<std::string>(dict["port_type"]);
                PortType port_type = (port_str == "INPUT") ? PortType::INPUT : PortType::OUTPUT;

                std::vector<size_t> indices;
                auto indices_list = nb::cast<nb::list>(dict["indices"]);
                for (size_t i = 0; i < nb::len(indices_list); ++i) {
                    indices.push_back(nb::cast<size_t>(indices_list[i]));
                }

                FQReference fq = FQReference::peered(node_id, port_type, std::move(indices));
                ref = TSReference::from_fq(fq, nullptr);  // Graph resolution deferred
                return;
            }
        }
    }

    // Import Python TimeSeriesReference module
    auto ref_module = nb::module_::import_("hgraph._types._ref_type");
    auto ts_ref_type = ref_module.attr("TimeSeriesReference");

    // Check if it's a Python TimeSeriesReference instance
    if (nb::isinstance(src, ts_ref_type)) {
        // Check is_empty property
        if (nb::cast<bool>(src.attr("is_empty"))) {
            ref = TSReference::empty();
            return;
        }

        // Check has_output (BoundTimeSeriesReference)
        if (nb::cast<bool>(src.attr("has_output"))) {
            // This is a BoundTimeSeriesReference - we need to extract the output's path
            // Get the output property
            nb::object output_obj = src.attr("output");

            // Try to get the short_path if the output has a TSOutputView
            // This requires the output to have a short_path() method (C++ wrapper)
            if (nb::hasattr(output_obj, "_short_path_for_ref")) {
                // The output wrapper provides a helper method that returns the ShortPath
                // as a tuple: (node_ptr, port_type, indices)
                try {
                    nb::tuple path_tuple = nb::cast<nb::tuple>(output_obj.attr("_short_path_for_ref")());
                    // path_tuple is (node, port_type, indices)
                    // For now, we can't easily reconstruct the ShortPath without the Node pointer
                    // Fall back to storing as FQReference
                    int node_id = 0;  // We don't have a reliable way to get node_id here
                    PortType port_type = PortType::OUTPUT;

                    std::vector<size_t> indices;
                    nb::list indices_list = nb::cast<nb::list>(path_tuple[2]);
                    for (size_t i = 0; i < nb::len(indices_list); ++i) {
                        indices.push_back(nb::cast<size_t>(indices_list[i]));
                    }

                    FQReference fq = FQReference::peered(node_id, port_type, std::move(indices));
                    ref = TSReference::from_fq(fq, nullptr);
                    return;
                } catch (...) {
                    // Fall through to empty reference
                }
            }

            // Couldn't extract path - create empty reference
            // TODO: Improve this to properly extract path from Python outputs
            ref = TSReference::empty();
            return;
        }

        // Otherwise it's UnBoundTimeSeriesReference with items
        // TODO: Handle UnBoundTimeSeriesReference
        ref = TSReference::empty();
        return;
    }

    // Unknown type - default to empty
    ref = TSReference::empty();
}

} // namespace hgraph::value
