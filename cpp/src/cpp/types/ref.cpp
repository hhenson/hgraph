#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

#include <algorithm>
#include <fmt/format.h>

namespace hgraph
{
    // ============================================================
    // TimeSeriesReference Implementation
    // ============================================================

    // Default constructor - EMPTY
    TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {}

    // BOUND constructor - node + path
    TimeSeriesReference::TimeSeriesReference(std::weak_ptr<Node> node, std::vector<PathKey> path)
        : _kind(Kind::BOUND), _node_ref(std::move(node)), _path(std::move(path)) {}

    // UNBOUND constructor - vector of references
    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items)
        : _kind(Kind::UNBOUND), _items(std::move(items)) {}

    // Check validity - node must still be alive
    bool TimeSeriesReference::valid() const {
        switch (_kind) {
            case Kind::EMPTY:
                return false;
            case Kind::BOUND:
                return !_node_ref.expired();
            case Kind::UNBOUND:
                return std::any_of(_items.begin(), _items.end(),
                                   [](const auto &item) { return item.valid(); });
        }
        return false;
    }

    // Get the node (lock weak_ptr)
    std::shared_ptr<Node> TimeSeriesReference::node() const {
        if (_kind != Kind::BOUND) return nullptr;
        return _node_ref.lock();
    }

    // Resolve to TimeSeriesValueView
    value::TimeSeriesValueView TimeSeriesReference::resolve() const {
        if (_kind != Kind::BOUND) {
            return {};  // Invalid view
        }

        auto n = _node_ref.lock();
        if (!n) {
            return {};  // Node expired
        }

        ts::TSOutput* output = n->output();
        if (!output) {
            return {};  // No output on node
        }

        // Start with the root view
        value::TSView view = output->view();

        // If no path, return the root view directly
        // Don't check view.valid() - collection types may have null value schema
        // but still be valid references to outputs
        if (_path.empty()) {
            return view;
        }

        // Navigate the path
        for (const auto& key : _path) {
            // For navigation, check if we have metadata rather than value validity
            // Collection types don't have value schemas but can still be navigated
            if (!view.ts_meta()) {
                return {};  // Can't navigate without type metadata
            }
            if (std::holds_alternative<size_t>(key)) {
                size_t idx = std::get<size_t>(key);
                // Try field first (for bundles), then element (for lists)
                if (view.kind() == value::TypeKind::Bundle) {
                    view = view.field(idx);
                } else {
                    view = view.element(idx);
                }
            } else {
                // TypeErasedKey - for TSD navigation
                // TODO: Implement TSD key-based navigation when needed
                return {};  // Not yet supported
            }
        }

        return view;
    }

    // Resolve to raw output pointer
    ts::TSOutput* TimeSeriesReference::output_ptr() const {
        if (_kind != Kind::BOUND) {
            return nullptr;
        }

        auto n = _node_ref.lock();
        if (!n) {
            return nullptr;  // Node expired
        }

        return n->output();  // Return root output (path navigation via resolve())
    }

    // Get items for UNBOUND references
    const std::vector<TimeSeriesReference>& TimeSeriesReference::items() const {
        if (_kind != Kind::UNBOUND) {
            throw std::runtime_error("TimeSeriesReference::items() called on non-unbound reference");
        }
        return _items;
    }

    const TimeSeriesReference& TimeSeriesReference::operator[](size_t ndx) const {
        return items()[ndx];
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const {
        if (_kind != other._kind) return false;

        switch (_kind) {
            case Kind::EMPTY:
                return true;
            case Kind::BOUND:
                {
                    auto this_node = _node_ref.lock();
                    auto other_node = other._node_ref.lock();
                    if (!this_node || !other_node) return false;
                    if (this_node != other_node) return false;
                    return _path == other._path;
                }
            case Kind::UNBOUND:
                return _items == other._items;
        }
        return false;
    }

    std::string TimeSeriesReference::to_string() const {
        switch (_kind) {
            case Kind::EMPTY:
                return "REF[<UnSet>]";
            case Kind::BOUND:
                {
                    auto n = _node_ref.lock();
                    if (!n) return "REF[<Expired>]";

                    std::string path_str;
                    for (size_t i = 0; i < _path.size(); ++i) {
                        if (i > 0) path_str += ".";
                        const auto& key = _path[i];
                        if (std::holds_alternative<size_t>(key)) {
                            path_str += std::to_string(std::get<size_t>(key));
                        } else {
                            path_str += "<key>";
                        }
                    }
                    if (path_str.empty()) {
                        return fmt::format("REF[{}<{}>]", n->signature().name,
                                           fmt::join(n->node_id(), ", "));
                    }
                    return fmt::format("REF[{}<{}>.path({})]", n->signature().name,
                                       fmt::join(n->node_id(), ", "), path_str);
                }
            case Kind::UNBOUND:
                {
                    std::vector<std::string> string_items;
                    string_items.reserve(_items.size());
                    for (const auto &item : _items) {
                        string_items.push_back(item.to_string());
                    }
                    return fmt::format("REF[{}]", fmt::join(string_items, ", "));
                }
        }
        return "REF[?]";
    }

    // Factory methods
    TimeSeriesReference TimeSeriesReference::make() {
        return TimeSeriesReference();
    }

    TimeSeriesReference TimeSeriesReference::make(std::weak_ptr<Node> node, std::vector<PathKey> path) {
        if (node.expired()) return make();
        return TimeSeriesReference(std::move(node), std::move(path));
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items) {
        if (items.empty()) return make();
        return TimeSeriesReference(std::move(items));
    }

    TimeSeriesReference TimeSeriesReference::make(const value::TSView& view) {
        // For TimeSeriesReference, we don't need view.valid() (which checks value/schema).
        // Collection types (TSL, TSD, etc.) have null schema but can still be referenced.
        // We just need to be able to navigate back to the owning node via the path.
        const auto& value_path = view.path();
        const auto* root = value_path.root_output();
        if (!root) return make();

        auto* node = root->owning_node();
        if (!node) return make();

        // Convert ValuePath indices to PathKeys
        std::vector<PathKey> path;
        path.reserve(value_path.depth());
        for (size_t i = 0; i < value_path.depth(); ++i) {
            // ValuePath stores indices directly
            path.push_back(value_path[i]);
        }

        return make(node->shared_from_this(), std::move(path));
    }

}  // namespace hgraph
