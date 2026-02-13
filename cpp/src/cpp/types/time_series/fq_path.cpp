/**
 * @file fq_path.cpp
 * @brief Implementation of FQPath and PathElement.
 */

#include <hgraph/types/time_series/fq_path.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/value/map_storage.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <sstream>
#include <stdexcept>

namespace nb = nanobind;

namespace hgraph {

// ============================================================================
// PathElement Implementation
// ============================================================================

PathElement::PathElement(const PathElement& other) {
    if (other.is_field()) {
        element_ = other.as_field();
    } else if (other.is_index()) {
        element_ = other.as_index();
    } else if (other.is_key()) {
        // Clone the key Value
        element_ = value::Value<>::copy(other.as_key());
    }
}

PathElement& PathElement::operator=(const PathElement& other) {
    if (this != &other) {
        if (other.is_field()) {
            element_ = other.as_field();
        } else if (other.is_index()) {
            element_ = other.as_index();
        } else if (other.is_key()) {
            element_ = value::Value<>::copy(other.as_key());
        }
    }
    return *this;
}

PathElement PathElement::key_from_view(const value::View& key_view) {
    PathElement e;
    // Clone the key by creating a Value from the View
    e.element_ = value::Value<>(key_view);
    return e;
}

std::string PathElement::to_string() const {
    if (is_field()) {
        return as_field();
    } else if (is_index()) {
        return "[" + std::to_string(as_index()) + "]";
    } else {
        // Key - use the value's string representation
        const auto& key = as_key();
        if (key.schema() && key.schema()->ops && key.schema()->ops->to_string) {
            return "[" + key.schema()->ops->to_string(key.view().data(), key.schema()) + "]";
        }
        return "[<key>]";
    }
}

nb::object PathElement::to_python() const {
    if (is_field()) {
        return nb::str(as_field().c_str());
    } else if (is_index()) {
        return nb::int_(static_cast<int64_t>(as_index()));
    } else {
        // Key - convert to Python object
        const auto& key = as_key();
        if (key.schema() && key.schema()->ops && key.schema()->ops->to_python) {
            return key.schema()->ops->to_python(key.view().data(), key.schema());
        }
        return nb::none();
    }
}

bool PathElement::operator==(const PathElement& other) const {
    if (is_field() && other.is_field()) {
        return as_field() == other.as_field();
    } else if (is_index() && other.is_index()) {
        return as_index() == other.as_index();
    } else if (is_key() && other.is_key()) {
        // Compare keys using their equals operation
        const auto& key1 = as_key();
        const auto& key2 = other.as_key();
        if (key1.schema() != key2.schema()) return false;
        if (key1.schema() && key1.schema()->ops && key1.schema()->ops->equals) {
            return key1.schema()->ops->equals(key1.view().data(), key2.view().data(), key1.schema());
        }
        return false;
    }
    return false;
}

// ============================================================================
// FQPath Implementation
// ============================================================================

std::string FQPath::to_string() const {
    std::ostringstream oss;

    // Node ID as array notation
    oss << "[";
    for (size_t i = 0; i < node_id_.size(); ++i) {
        if (i > 0) oss << ",";
        oss << node_id_[i];
    }
    oss << "]";

    // Port type
    oss << "." << (port_type_ == PortType::INPUT ? "in" : "out");

    // Path elements
    for (const auto& elem : path_) {
        if (elem.is_field()) {
            oss << "." << elem.as_field();
        } else {
            oss << elem.to_string();
        }
    }

    return oss.str();
}

nb::object FQPath::to_python() const {
    // Return as tuple: (node_id_list, port_type_str, path_list)
    nb::list node_id_list;
    for (auto id : node_id_) {
        node_id_list.append(nb::int_(id));
    }

    nb::str port_type_str = port_type_ == PortType::INPUT ? nb::str("INPUT") : nb::str("OUTPUT");

    nb::list path_list;
    for (const auto& elem : path_) {
        path_list.append(elem.to_python());
    }

    return nb::make_tuple(node_id_list, port_type_str, path_list);
}

bool FQPath::operator==(const FQPath& other) const {
    if (node_id_ != other.node_id_) return false;
    if (port_type_ != other.port_type_) return false;
    if (path_.size() != other.path_.size()) return false;

    for (size_t i = 0; i < path_.size(); ++i) {
        if (path_[i] != other.path_[i]) return false;
    }

    return true;
}

bool FQPath::operator<(const FQPath& other) const {
    // Compare node_id first
    if (node_id_ < other.node_id_) return true;
    if (node_id_ > other.node_id_) return false;

    // Then port type
    if (port_type_ < other.port_type_) return true;
    if (port_type_ > other.port_type_) return false;

    // Then path elements by depth
    size_t min_depth = std::min(path_.size(), other.path_.size());
    for (size_t i = 0; i < min_depth; ++i) {
        const auto& a = path_[i];
        const auto& b = other.path_[i];

        // Different types: field < index < key
        if (a.is_field() && !b.is_field()) return true;
        if (!a.is_field() && b.is_field()) return false;
        if (a.is_index() && b.is_key()) return true;
        if (a.is_key() && b.is_index()) return false;

        // Same type: compare values
        if (a.is_field() && b.is_field()) {
            if (a.as_field() < b.as_field()) return true;
            if (a.as_field() > b.as_field()) return false;
        } else if (a.is_index() && b.is_index()) {
            if (a.as_index() < b.as_index()) return true;
            if (a.as_index() > b.as_index()) return false;
        }
        // Keys: just use equality check for now
    }

    // Shorter path is less
    return path_.size() < other.path_.size();
}

} // namespace hgraph
