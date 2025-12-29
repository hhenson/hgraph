#pragma once

/**
 * @file path.h
 * @brief Path-based navigation for nested Value structures.
 *
 * Enables navigation through nested structures using path expressions like:
 * - "user.name" (field access)
 * - "items[0]" (index access)
 * - "users[0].addresses[1].city" (mixed access)
 *
 * Reference: ts_design_docs/Value_USER_GUIDE.md Section 10
 *
 * Usage:
 * @code
 * // Parse a path
 * ValuePath path = parse_path("user.address.city");
 *
 * // Navigate through structure
 * ConstValueView city = navigate(root, path);
 *
 * // Or use try_navigate for safe access
 * auto maybe_city = try_navigate(root, path);
 * if (maybe_city) {
 *     std::cout << maybe_city->as<std::string>() << std::endl;
 * }
 * @endcode
 */

#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/value_storage.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace hgraph::value {

// ============================================================================
// PathElement - Represents a single step in a path
// ============================================================================

/**
 * @brief Holds a value key for map navigation.
 *
 * This wraps ValueStorage to provide copyable semantics via shared_ptr.
 * Used when navigating maps with arbitrary key types.
 */
struct ValueKeyHolder {
    std::shared_ptr<ValueStorage> storage;
    const TypeMeta* schema = nullptr;

    ValueKeyHolder() = default;

    ValueKeyHolder(const TypeMeta* s) : storage(std::make_shared<ValueStorage>()), schema(s) {
        if (s) {
            storage->construct(s);
        }
    }

    /// Create from a ConstValueView (copies the data)
    static ValueKeyHolder from_view(ConstValueView view) {
        ValueKeyHolder holder;
        holder.schema = view.schema();
        holder.storage = std::make_shared<ValueStorage>();
        holder.storage->construct(view.schema());
        view.schema()->ops->copy_assign(holder.storage->data(), view.data(), view.schema());
        return holder;
    }

    [[nodiscard]] ConstValueView view() const {
        return ConstValueView(storage->data(), schema);
    }

    [[nodiscard]] bool valid() const {
        return storage && schema;
    }
};

/**
 * @brief Represents a single element in a navigation path.
 *
 * A path element can be:
 * - A field name (string) - for bundle field access
 * - An index (size_t) - for tuple/list element access
 * - A value key (ValueKeyHolder) - for map key access with any key type
 *
 * The interpretation depends on the target type during navigation:
 * - String on bundle -> field access by name
 * - String on map with string keys -> string key lookup
 * - Index on list/tuple -> index access
 * - Index on map with integer keys -> integer key lookup
 * - Value on map -> value key lookup (matches map's key type)
 */
class PathElement {
public:
    // ========== Factory Methods ==========

    /**
     * @brief Create a field access element (for bundles).
     * @param name The field name
     * @return A field path element
     */
    static PathElement field(std::string name) {
        PathElement elem;
        elem._data = std::move(name);
        return elem;
    }

    /**
     * @brief Create an index access element.
     * For lists/tuples: index access
     * For maps with integer keys: converted to int64_t key
     * @param idx The index or integer key
     * @return An index path element
     */
    static PathElement index(size_t idx) {
        PathElement elem;
        elem._data = idx;
        return elem;
    }

    /**
     * @brief Create a value key element (for maps with arbitrary key types).
     * @param view The key value (will be copied)
     * @return A value key path element
     */
    static PathElement key(ConstValueView view) {
        PathElement elem;
        elem._data = ValueKeyHolder::from_view(view);
        return elem;
    }

    // ========== Type Queries ==========

    /**
     * @brief Check if this is a string-based element (field name).
     * @return true if this uses a string for access
     */
    [[nodiscard]] bool is_string() const noexcept {
        return std::holds_alternative<std::string>(_data);
    }

    /**
     * @brief Check if this is a field access element.
     * Alias for is_string().
     * @return true if this accesses by field name
     */
    [[nodiscard]] bool is_field() const noexcept {
        return is_string();
    }

    /**
     * @brief Check if this is an index/integer access element.
     * @return true if this accesses by index
     */
    [[nodiscard]] bool is_index() const noexcept {
        return std::holds_alternative<size_t>(_data);
    }

    /**
     * @brief Check if this is a value key element.
     * @return true if this contains an arbitrary value key
     */
    [[nodiscard]] bool is_value() const noexcept {
        return std::holds_alternative<ValueKeyHolder>(_data);
    }

    // ========== Accessors ==========

    /**
     * @brief Get the string value (field name).
     * @return The field name
     * @throws std::runtime_error if not a string element
     */
    [[nodiscard]] const std::string& name() const {
        if (!is_string()) {
            throw std::runtime_error("PathElement is not a string element");
        }
        return std::get<std::string>(_data);
    }

    /**
     * @brief Get the index value.
     * @return The index
     * @throws std::runtime_error if not an index element
     */
    [[nodiscard]] size_t get_index() const {
        if (!is_index()) {
            throw std::runtime_error("PathElement is not an index element");
        }
        return std::get<size_t>(_data);
    }

    /**
     * @brief Get the value key as a view.
     * @return ConstValueView of the key value
     * @throws std::runtime_error if not a value element
     */
    [[nodiscard]] ConstValueView get_value() const {
        if (!is_value()) {
            throw std::runtime_error("PathElement is not a value element");
        }
        return std::get<ValueKeyHolder>(_data).view();
    }

    /**
     * @brief Get the value key's schema.
     * @return The TypeMeta* for the value key, or nullptr if not a value element
     */
    [[nodiscard]] const TypeMeta* value_schema() const noexcept {
        if (!is_value()) {
            return nullptr;
        }
        return std::get<ValueKeyHolder>(_data).schema;
    }

    /**
     * @brief Convert to string representation.
     * @return String like "field_name", "[0]", or "[<value>]"
     */
    [[nodiscard]] std::string to_string() const {
        if (is_string()) {
            return std::get<std::string>(_data);
        } else if (is_index()) {
            return "[" + std::to_string(std::get<size_t>(_data)) + "]";
        } else {
            // Value key - show type and value
            auto view = get_value();
            return "[" + view.to_string() + "]";
        }
    }

private:
    PathElement() = default;
    std::variant<std::string, size_t, ValueKeyHolder> _data;
};

// ============================================================================
// ValuePath - A sequence of path elements
// ============================================================================

/**
 * @brief A path through a nested value structure.
 *
 * ValuePath is a sequence of PathElements that describe how to navigate
 * from a root value to a nested element.
 */
using ValuePath = std::vector<PathElement>;

// ============================================================================
// Path Parsing
// ============================================================================

/**
 * @brief Parse a path string into a ValuePath.
 *
 * Supports the following syntax:
 * - Field access: "name", "user.address"
 * - Index access: "[0]", "items[0]"
 * - String key access: "[\"key\"]", "['key']", "map[\"mykey\"]"
 * - Mixed: "users[0].addresses[1].city", "data[\"key\"].value"
 *
 * @param path_str The path string to parse
 * @return The parsed path
 * @throws std::runtime_error on invalid syntax
 */
inline ValuePath parse_path(std::string_view path_str) {
    ValuePath path;

    if (path_str.empty()) {
        return path;
    }

    size_t pos = 0;
    const size_t len = path_str.length();

    // Check for leading dot
    if (path_str[0] == '.') {
        throw std::runtime_error("Invalid path: leading dot");
    }

    while (pos < len) {
        // Skip dots between elements (not at start)
        if (pos > 0 && path_str[pos] == '.') {
            ++pos;
            if (pos >= len) {
                throw std::runtime_error("Invalid path: trailing dot");
            }
            // Check for double dots
            if (path_str[pos] == '.') {
                throw std::runtime_error("Invalid path: consecutive dots");
            }
        }

        if (path_str[pos] == '[') {
            // Bracket access - could be index or string key
            ++pos;
            if (pos >= len) {
                throw std::runtime_error("Invalid path: unclosed bracket");
            }

            // Check for quoted string key
            char quote_char = 0;
            if (path_str[pos] == '"' || path_str[pos] == '\'') {
                quote_char = path_str[pos];
                ++pos;
                if (pos >= len) {
                    throw std::runtime_error("Invalid path: unclosed string key");
                }

                // Find closing quote
                size_t key_start = pos;
                while (pos < len && path_str[pos] != quote_char) {
                    // Handle escaped quotes
                    if (path_str[pos] == '\\' && pos + 1 < len) {
                        pos += 2;
                    } else {
                        ++pos;
                    }
                }

                if (pos >= len) {
                    throw std::runtime_error("Invalid path: unclosed string key");
                }

                std::string key_str(path_str.substr(key_start, pos - key_start));
                ++pos; // Skip closing quote

                // Expect closing bracket
                if (pos >= len || path_str[pos] != ']') {
                    throw std::runtime_error("Invalid path: expected ] after string key");
                }
                ++pos; // Skip ]

                // String key is stored as field - navigate() handles map string keys
                path.push_back(PathElement::field(std::move(key_str)));
            } else {
                // Numeric index
                size_t end_bracket = path_str.find(']', pos);
                if (end_bracket == std::string_view::npos) {
                    throw std::runtime_error("Invalid path: unclosed bracket");
                }

                std::string_view index_str = path_str.substr(pos, end_bracket - pos);
                if (index_str.empty()) {
                    throw std::runtime_error("Invalid path: empty index");
                }

                // Parse index (must be numeric)
                size_t idx = 0;
                for (char c : index_str) {
                    if (c == '-') {
                        throw std::runtime_error("Invalid path: negative index");
                    }
                    if (!std::isdigit(static_cast<unsigned char>(c))) {
                        throw std::runtime_error("Invalid path: non-numeric index");
                    }
                    idx = idx * 10 + (c - '0');
                }

                path.push_back(PathElement::index(idx));
                pos = end_bracket + 1;
            }

            // Check for consecutive brackets (e.g., matrix[0][1] or map["a"]["b"])
            if (pos < len && path_str[pos] == '[') {
                continue;
            }
        } else if (path_str[pos] == ']') {
            throw std::runtime_error("Invalid path: unexpected closing bracket");
        } else if (std::isspace(static_cast<unsigned char>(path_str[pos]))) {
            throw std::runtime_error("Invalid path: whitespace not allowed");
        } else {
            // Field name
            size_t name_start = pos;

            // Find end of field name
            while (pos < len &&
                   path_str[pos] != '.' &&
                   path_str[pos] != '[' &&
                   path_str[pos] != ']' &&
                   !std::isspace(static_cast<unsigned char>(path_str[pos]))) {
                ++pos;
            }

            if (pos > name_start) {
                std::string field_name(path_str.substr(name_start, pos - name_start));
                path.push_back(PathElement::field(std::move(field_name)));
            }
        }
    }

    return path;
}

/**
 * @brief Convert a path back to string representation.
 *
 * @param path The path to stringify
 * @return The path string
 */
inline std::string path_to_string(const ValuePath& path) {
    std::string result;
    bool first = true;

    for (const auto& elem : path) {
        if (elem.is_field()) {
            if (!first && !result.empty() && result.back() != ']') {
                result += '.';
            }
            result += elem.name();
        } else {
            result += '[';
            result += std::to_string(elem.get_index());
            result += ']';
        }
        first = false;
    }

    return result;
}

// ============================================================================
// Navigation Functions
// ============================================================================

/**
 * @brief Navigate through a value using a path.
 *
 * Path element handling depends on both the element type and target type:
 *
 * String elements:
 * - On bundle -> field access by name
 * - On map with string keys -> string key lookup
 * - Otherwise -> error
 *
 * Index elements:
 * - On tuple/list -> index access
 * - On bundle -> index access (by field position)
 * - On map with integer keys -> integer key lookup
 * - Otherwise -> error
 *
 * Value elements:
 * - On map -> lookup using the value as key (must match map's key type)
 * - Otherwise -> error
 *
 * @param view The root value view
 * @param path The path to navigate
 * @return The view at the path destination
 * @throws std::runtime_error if navigation fails
 */
inline ConstValueView navigate(ConstValueView view, const ValuePath& path) {
    ConstValueView current = view;

    for (const auto& elem : path) {
        if (!current.valid()) {
            throw std::runtime_error("Navigation failed: invalid view");
        }

        if (elem.is_value()) {
            // Value key - only valid for maps
            if (!current.is_map()) {
                throw std::runtime_error(
                    "Navigation failed: value key access requires map");
            }
            auto map = current.as_map();
            ConstValueView key_view = elem.get_value();

            // Verify key type matches map's key type
            if (key_view.schema() != map.schema()->key_type) {
                throw std::runtime_error(
                    "Navigation failed: key type mismatch for map access");
            }
            current = map.at(key_view);
        } else if (elem.is_string()) {
            // String access - Bundle field or Map with string keys
            const std::string& field_name = elem.name();

            if (current.is_bundle()) {
                auto bundle = current.as_bundle();
                current = bundle.at(field_name);
            } else if (current.is_map()) {
                auto map = current.as_map();
                // Check if map has string keys
                if (map.schema()->key_type != scalar_type_meta<std::string>()) {
                    throw std::runtime_error(
                        "Navigation failed: map does not have string keys");
                }
                current = map.at(ConstValueView(&field_name, scalar_type_meta<std::string>()));
            } else {
                throw std::runtime_error(
                    "Navigation failed: string access requires bundle or map with string keys");
            }
        } else {
            // Index access - Tuple, List, Bundle, or Map with integer keys
            size_t idx = elem.get_index();

            if (current.is_tuple()) {
                auto tuple = current.as_tuple();
                if (idx >= tuple.size()) {
                    throw std::runtime_error("Navigation failed: tuple index out of range");
                }
                current = tuple[idx];
            } else if (current.is_list()) {
                auto list = current.as_list();
                if (idx >= list.size()) {
                    throw std::runtime_error("Navigation failed: list index out of range");
                }
                current = list[idx];
            } else if (current.is_bundle()) {
                auto bundle = current.as_bundle();
                if (idx >= bundle.size()) {
                    throw std::runtime_error("Navigation failed: bundle index out of range");
                }
                current = bundle[idx];
            } else if (current.is_map()) {
                auto map = current.as_map();
                const TypeMeta* key_type = map.schema()->key_type;

                // Check if map has integer keys
                if (key_type == scalar_type_meta<int64_t>()) {
                    int64_t int_key = static_cast<int64_t>(idx);
                    current = map.at(ConstValueView(&int_key, key_type));
                } else if (key_type == scalar_type_meta<size_t>()) {
                    current = map.at(ConstValueView(&idx, key_type));
                } else if (key_type == scalar_type_meta<int32_t>()) {
                    int32_t int_key = static_cast<int32_t>(idx);
                    current = map.at(ConstValueView(&int_key, key_type));
                } else {
                    throw std::runtime_error(
                        "Navigation failed: map does not have integer keys");
                }
            } else {
                throw std::runtime_error(
                    "Navigation failed: index access on non-indexable type");
            }
        }
    }

    return current;
}

/**
 * @brief Navigate through a value using a path string.
 *
 * @param view The root value view
 * @param path_str The path string to parse and navigate
 * @return The view at the path destination
 * @throws std::runtime_error if parsing or navigation fails
 */
inline ConstValueView navigate(ConstValueView view, std::string_view path_str) {
    return navigate(view, parse_path(path_str));
}

/**
 * @brief Try to navigate through a value using a path.
 *
 * Returns nullopt instead of throwing on failure.
 *
 * @param view The root value view
 * @param path The path to navigate
 * @return The view at the path destination, or nullopt on failure
 */
inline std::optional<ConstValueView> try_navigate(ConstValueView view, const ValuePath& path) {
    try {
        return navigate(view, path);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

/**
 * @brief Try to navigate through a value using a path string.
 *
 * @param view The root value view
 * @param path_str The path string to parse and navigate
 * @return The view at the path destination, or nullopt on failure
 */
inline std::optional<ConstValueView> try_navigate(ConstValueView view, std::string_view path_str) {
    try {
        return navigate(view, parse_path(path_str));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// ============================================================================
// Mutable Navigation Functions
// ============================================================================

/**
 * @brief Navigate through a mutable value using a path.
 *
 * Path element handling depends on both the element type and target type:
 *
 * String elements:
 * - On bundle -> field access by name
 * - On map with string keys -> string key lookup
 * - Otherwise -> error
 *
 * Index elements:
 * - On tuple/list -> index access
 * - On bundle -> index access (by field position)
 * - On map with integer keys -> integer key lookup
 * - Otherwise -> error
 *
 * Value elements:
 * - On map -> lookup using the value as key (must match map's key type)
 * - Otherwise -> error
 *
 * @param view The root mutable value view
 * @param path The path to navigate
 * @return The mutable view at the path destination
 * @throws std::runtime_error if navigation fails
 */
inline ValueView navigate_mut(ValueView view, const ValuePath& path) {
    ValueView current = view;

    for (const auto& elem : path) {
        if (!current.valid()) {
            throw std::runtime_error("Navigation failed: invalid view");
        }

        if (elem.is_value()) {
            // Value key - only valid for maps
            if (!current.is_map()) {
                throw std::runtime_error(
                    "Navigation failed: value key access requires map");
            }
            auto map = current.as_map();
            ConstValueView key_view = elem.get_value();

            // Verify key type matches map's key type
            if (key_view.schema() != map.schema()->key_type) {
                throw std::runtime_error(
                    "Navigation failed: key type mismatch for map access");
            }
            current = map.at(key_view);
        } else if (elem.is_string()) {
            // String access - Bundle field or Map with string keys
            const std::string& field_name = elem.name();

            if (current.is_bundle()) {
                auto bundle = current.as_bundle();
                current = bundle.at(field_name);
            } else if (current.is_map()) {
                auto map = current.as_map();
                // Check if map has string keys
                if (map.schema()->key_type != scalar_type_meta<std::string>()) {
                    throw std::runtime_error(
                        "Navigation failed: map does not have string keys");
                }
                current = map.at(ConstValueView(&field_name, scalar_type_meta<std::string>()));
            } else {
                throw std::runtime_error(
                    "Navigation failed: string access requires bundle or map with string keys");
            }
        } else {
            // Index access - Tuple, List, Bundle, or Map with integer keys
            size_t idx = elem.get_index();

            if (current.is_tuple()) {
                auto tuple = current.as_tuple();
                if (idx >= tuple.size()) {
                    throw std::runtime_error("Navigation failed: tuple index out of range");
                }
                current = tuple.at(idx);
            } else if (current.is_list()) {
                auto list = current.as_list();
                if (idx >= list.size()) {
                    throw std::runtime_error("Navigation failed: list index out of range");
                }
                current = list.at(idx);
            } else if (current.is_bundle()) {
                auto bundle = current.as_bundle();
                if (idx >= bundle.size()) {
                    throw std::runtime_error("Navigation failed: bundle index out of range");
                }
                current = bundle.at(idx);
            } else if (current.is_map()) {
                auto map = current.as_map();
                const TypeMeta* key_type = map.schema()->key_type;

                // Check if map has integer keys
                if (key_type == scalar_type_meta<int64_t>()) {
                    int64_t int_key = static_cast<int64_t>(idx);
                    current = map.at(ConstValueView(&int_key, key_type));
                } else if (key_type == scalar_type_meta<size_t>()) {
                    current = map.at(ConstValueView(&idx, key_type));
                } else if (key_type == scalar_type_meta<int32_t>()) {
                    int32_t int_key = static_cast<int32_t>(idx);
                    current = map.at(ConstValueView(&int_key, key_type));
                } else {
                    throw std::runtime_error(
                        "Navigation failed: map does not have integer keys");
                }
            } else {
                throw std::runtime_error(
                    "Navigation failed: index access on non-indexable type");
            }
        }
    }

    return current;
}

/**
 * @brief Navigate through a mutable value using a path string.
 *
 * @param view The root mutable value view
 * @param path_str The path string to parse and navigate
 * @return The mutable view at the path destination
 * @throws std::runtime_error if parsing or navigation fails
 */
inline ValueView navigate_mut(ValueView view, std::string_view path_str) {
    return navigate_mut(view, parse_path(path_str));
}

/**
 * @brief Try to navigate through a mutable value using a path.
 *
 * @param view The root mutable value view
 * @param path The path to navigate
 * @return The mutable view at the path destination, or nullopt on failure
 */
inline std::optional<ValueView> try_navigate_mut(ValueView view, const ValuePath& path) {
    try {
        return navigate_mut(view, path);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

/**
 * @brief Try to navigate through a mutable value using a path string.
 *
 * @param view The root mutable value view
 * @param path_str The path string to parse and navigate
 * @return The mutable view at the path destination, or nullopt on failure
 */
inline std::optional<ValueView> try_navigate_mut(ValueView view, std::string_view path_str) {
    try {
        return navigate_mut(view, parse_path(path_str));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

} // namespace hgraph::value
