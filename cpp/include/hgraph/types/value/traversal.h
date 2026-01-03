#pragma once

/**
 * @file traversal.h
 * @brief Deep traversal utilities for nested Value structures.
 *
 * Enables recursive visiting of all leaf values in nested structures,
 * tracking the path to each leaf. Useful for:
 * - Counting elements
 * - Collecting values with their paths
 * - Transforming all scalars
 * - Serialization/deserialization
 *
 * Reference: ts_design_docs/Value_USER_GUIDE.md Section 9
 *
 * Usage:
 * @code
 * // Count all leaf values
 * size_t count = count_leaves(value.const_view());
 *
 * // Visit all leaves with path
 * deep_visit(value.const_view(), [](ConstValueView leaf, const TraversalPath& path) {
 *     std::cout << "At path " << path_to_string(path) << ": ";
 *     std::cout << leaf.to_string() << std::endl;
 * });
 *
 * // Collect all paths
 * auto paths = collect_leaf_paths(value.const_view());
 * @endcode
 */

#include <hgraph/types/value/path.h>

#include <functional>
#include <variant>
#include <vector>

namespace hgraph::value {

// ============================================================================
// Traversal Path Element
// ============================================================================

/**
 * @brief A single element in a traversal path.
 *
 * Can be either a field name (string) or an index (size_t).
 * This is more efficient than PathElement for traversal since it
 * doesn't need factory methods.
 */
using TraversalPathElement = std::variant<std::string, size_t>;

/**
 * @brief A path through a nested structure during traversal.
 *
 * Each element is either a string (field name) or size_t (index).
 */
using TraversalPath = std::vector<TraversalPathElement>;

// ============================================================================
// Traversal Path Utilities
// ============================================================================

/**
 * @brief Convert a traversal path element to string.
 *
 * @param elem The path element
 * @return String representation
 */
inline std::string to_string(const TraversalPathElement& elem) {
    if (std::holds_alternative<std::string>(elem)) {
        return std::get<std::string>(elem);
    } else {
        return std::to_string(std::get<size_t>(elem));
    }
}

/**
 * @brief Convert a traversal path to a dot-notation string.
 *
 * @param path The traversal path
 * @return String like "field1.field2[0].field3"
 */
inline std::string path_to_string(const TraversalPath& path) {
    std::string result;
    bool first = true;

    for (const auto& elem : path) {
        if (std::holds_alternative<std::string>(elem)) {
            if (!first && !result.empty() && result.back() != ']') {
                result += '.';
            }
            result += std::get<std::string>(elem);
        } else {
            result += '[';
            result += std::to_string(std::get<size_t>(elem));
            result += ']';
        }
        first = false;
    }

    return result;
}

// ============================================================================
// Deep Traversal Callback Type
// ============================================================================

/**
 * @brief Callback type for deep_visit.
 *
 * Called for each leaf (scalar) value with the value and its path.
 */
using DeepVisitCallback = std::function<void(ConstValueView, const TraversalPath&)>;

// ============================================================================
// Deep Visit Implementation
// ============================================================================

namespace detail {

/**
 * @brief Recursive helper for deep traversal.
 */
inline void deep_visit_impl(
    ConstValueView view,
    TraversalPath& path,
    const DeepVisitCallback& callback
) {
    if (!view.valid()) {
        return;
    }

    switch (view.schema()->kind) {
        case TypeKind::Scalar:
            // Leaf node - call callback
            callback(view, path);
            break;

        case TypeKind::Tuple: {
            auto tuple = view.as_tuple();
            for (size_t i = 0; i < tuple.size(); ++i) {
                path.push_back(i);
                deep_visit_impl(tuple[i], path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::Bundle: {
            auto bundle = view.as_bundle();
            for (size_t i = 0; i < bundle.size(); ++i) {
                const BundleFieldInfo* info = bundle.field_info(i);
                path.push_back(std::string(info->name ? info->name : ""));
                deep_visit_impl(bundle[i], path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::List: {
            auto list = view.as_list();
            for (size_t i = 0; i < list.size(); ++i) {
                path.push_back(i);
                deep_visit_impl(list[i], path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::Set: {
            auto set = view.as_set();
            // Use TypeOps get_at for indexed access to set elements
            const auto* schema = set.schema();
            for (size_t i = 0; i < set.size(); ++i) {
                path.push_back(i);
                const void* elem_data = schema->ops->get_at(set.data(), i, schema);
                if (elem_data) {
                    deep_visit_impl(ConstValueView(elem_data, schema->element_type), path, callback);
                }
                path.pop_back();
            }
            break;
        }

        case TypeKind::Map: {
            auto map = view.as_map();
            // Iterate through keys using ConstKeySetView iterator
            auto key_set = map.keys();
            size_t i = 0;
            for (auto key : key_set) {
                auto value = map.at(key);

                // Try to use key's string value as path element if possible
                if (key.schema() == scalar_type_meta<std::string>()) {
                    path.push_back(key.as<std::string>());
                } else {
                    path.push_back(i);
                }
                deep_visit_impl(value, path, callback);
                path.pop_back();
                ++i;
            }
            break;
        }

        case TypeKind::CyclicBuffer: {
            auto buf = view.as_cyclic_buffer();
            for (size_t i = 0; i < buf.size(); ++i) {
                path.push_back(i);
                deep_visit_impl(buf[i], path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::Queue: {
            auto queue = view.as_queue();
            for (size_t i = 0; i < queue.size(); ++i) {
                path.push_back(i);
                deep_visit_impl(queue[i], path, callback);
                path.pop_back();
            }
            break;
        }

        default:
            // Ref or future types - no traversal
            break;
    }
}

} // namespace detail

// ============================================================================
// Public Deep Traversal Functions
// ============================================================================

/**
 * @brief Visit all leaf (scalar) values in a nested structure.
 *
 * Recursively traverses through composites (tuples, bundles, lists, sets, maps)
 * and calls the callback for each scalar value, providing the path to that value.
 *
 * @param view The root value to traverse
 * @param callback Called for each leaf with (leaf_view, path)
 */
inline void deep_visit(ConstValueView view, const DeepVisitCallback& callback) {
    TraversalPath path;
    detail::deep_visit_impl(view, path, callback);
}

/**
 * @brief Count all leaf (scalar) values in a nested structure.
 *
 * @param view The root value to traverse
 * @return The number of scalar values
 */
inline size_t count_leaves(ConstValueView view) {
    size_t count = 0;
    deep_visit(view, [&count](ConstValueView, const TraversalPath&) {
        ++count;
    });
    return count;
}

/**
 * @brief Collect the paths to all leaf values.
 *
 * @param view The root value to traverse
 * @return Vector of paths to each leaf
 */
inline std::vector<TraversalPath> collect_leaf_paths(ConstValueView view) {
    std::vector<TraversalPath> paths;
    deep_visit(view, [&paths](ConstValueView, const TraversalPath& path) {
        paths.push_back(path);
    });
    return paths;
}

/**
 * @brief Collect all leaf values with their paths.
 *
 * @param view The root value to traverse
 * @return Vector of (path, value_view) pairs
 */
inline std::vector<std::pair<TraversalPath, ConstValueView>> collect_leaves(ConstValueView view) {
    std::vector<std::pair<TraversalPath, ConstValueView>> leaves;
    deep_visit(view, [&leaves](ConstValueView leaf, const TraversalPath& path) {
        leaves.emplace_back(path, leaf);
    });
    return leaves;
}

// ============================================================================
// Mutable Deep Traversal
// ============================================================================

/**
 * @brief Callback type for mutable deep_visit.
 */
using MutableDeepVisitCallback = std::function<void(ValueView, const TraversalPath&)>;

namespace detail {

/**
 * @brief Recursive helper for mutable deep traversal.
 */
inline void deep_visit_mut_impl(
    ValueView view,
    TraversalPath& path,
    const MutableDeepVisitCallback& callback
) {
    if (!view.valid()) {
        return;
    }

    switch (view.schema()->kind) {
        case TypeKind::Scalar:
            callback(view, path);
            break;

        case TypeKind::Tuple: {
            auto tuple = view.as_tuple();
            for (size_t i = 0; i < tuple.size(); ++i) {
                path.push_back(i);
                deep_visit_mut_impl(tuple.at(i), path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::Bundle: {
            auto bundle = view.as_bundle();
            for (size_t i = 0; i < bundle.size(); ++i) {
                const BundleFieldInfo* info = bundle.field_info(i);
                path.push_back(std::string(info->name ? info->name : ""));
                deep_visit_mut_impl(bundle.at(i), path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::List: {
            auto list = view.as_list();
            for (size_t i = 0; i < list.size(); ++i) {
                path.push_back(i);
                deep_visit_mut_impl(list.at(i), path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::Set: {
            // Sets are not mutable during iteration - skip
            // Set elements cannot be modified in-place
            (void)view;
            break;
        }

        case TypeKind::Map: {
            auto map = view.as_map();
            // Iterate through keys using ConstKeySetView iterator
            auto key_set = map.keys();
            size_t i = 0;
            for (auto key : key_set) {
                if (key.schema() == scalar_type_meta<std::string>()) {
                    path.push_back(key.as<std::string>());
                } else {
                    path.push_back(i);
                }
                // Get mutable view for the value
                ValueView mut_value = map.at(key);
                deep_visit_mut_impl(mut_value, path, callback);
                path.pop_back();
                ++i;
            }
            break;
        }

        case TypeKind::CyclicBuffer: {
            auto buf = view.as_cyclic_buffer();
            for (size_t i = 0; i < buf.size(); ++i) {
                path.push_back(i);
                deep_visit_mut_impl(buf.at(i), path, callback);
                path.pop_back();
            }
            break;
        }

        case TypeKind::Queue: {
            auto queue = view.as_queue();
            for (size_t i = 0; i < queue.size(); ++i) {
                path.push_back(i);
                deep_visit_mut_impl(queue.at(i), path, callback);
                path.pop_back();
            }
            break;
        }

        default:
            break;
    }
}

} // namespace detail

/**
 * @brief Visit all leaf values with mutable access.
 *
 * Similar to deep_visit but provides mutable ValueView to the callback,
 * allowing in-place modification of leaf values.
 *
 * @param view The root mutable value to traverse
 * @param callback Called for each leaf with (mutable_leaf_view, path)
 */
inline void deep_visit_mut(ValueView view, const MutableDeepVisitCallback& callback) {
    TraversalPath path;
    detail::deep_visit_mut_impl(view, path, callback);
}

// ============================================================================
// Transformation Utilities
// ============================================================================

/**
 * @brief Apply a transformation to all numeric scalar values.
 *
 * @tparam F Function type: int64_t(int64_t) or double(double)
 * @param view The root mutable value to transform
 * @param transform_int Function to transform int64_t values
 * @param transform_double Function to transform double values
 */
template<typename IntFn, typename DoubleFn>
void transform_numeric(ValueView view, IntFn&& transform_int, DoubleFn&& transform_double) {
    deep_visit_mut(view, [&](ValueView leaf, const TraversalPath&) {
        if (leaf.is_scalar_type<int64_t>()) {
            leaf.as<int64_t>() = transform_int(leaf.as<int64_t>());
        } else if (leaf.is_scalar_type<double>()) {
            leaf.as<double>() = transform_double(leaf.as<double>());
        }
    });
}

/**
 * @brief Apply a transformation to all int64_t values.
 *
 * @tparam F Function type: int64_t(int64_t)
 * @param view The root mutable value to transform
 * @param transform Function to apply
 */
template<typename F>
void transform_int64(ValueView view, F&& transform) {
    deep_visit_mut(view, [&](ValueView leaf, const TraversalPath&) {
        if (leaf.is_scalar_type<int64_t>()) {
            leaf.as<int64_t>() = transform(leaf.as<int64_t>());
        }
    });
}

/**
 * @brief Apply a transformation to all double values.
 *
 * @tparam F Function type: double(double)
 * @param view The root mutable value to transform
 * @param transform Function to apply
 */
template<typename F>
void transform_double(ValueView view, F&& transform) {
    deep_visit_mut(view, [&](ValueView leaf, const TraversalPath&) {
        if (leaf.is_scalar_type<double>()) {
            leaf.as<double>() = transform(leaf.as<double>());
        }
    });
}

/**
 * @brief Apply a transformation to all string values.
 *
 * @tparam F Function type: std::string(const std::string&)
 * @param view The root mutable value to transform
 * @param transform Function to apply
 */
template<typename F>
void transform_string(ValueView view, F&& transform) {
    deep_visit_mut(view, [&](ValueView leaf, const TraversalPath&) {
        if (leaf.is_scalar_type<std::string>()) {
            leaf.as<std::string>() = transform(leaf.as<std::string>());
        }
    });
}

// ============================================================================
// Aggregation Utilities
// ============================================================================

/**
 * @brief Sum all numeric leaf values.
 *
 * @param view The root value to traverse
 * @return The sum of all int64_t and double values (as double)
 */
inline double sum_numeric(ConstValueView view) {
    double sum = 0.0;
    deep_visit(view, [&sum](ConstValueView leaf, const TraversalPath&) {
        if (leaf.is_scalar_type<int64_t>()) {
            sum += static_cast<double>(leaf.as<int64_t>());
        } else if (leaf.is_scalar_type<double>()) {
            sum += leaf.as<double>();
        }
    });
    return sum;
}

/**
 * @brief Find maximum numeric leaf value.
 *
 * @param view The root value to traverse
 * @return The maximum value, or nullopt if no numeric leaves
 */
inline std::optional<double> max_numeric(ConstValueView view) {
    std::optional<double> max_val;
    deep_visit(view, [&max_val](ConstValueView leaf, const TraversalPath&) {
        double val;
        if (leaf.is_scalar_type<int64_t>()) {
            val = static_cast<double>(leaf.as<int64_t>());
        } else if (leaf.is_scalar_type<double>()) {
            val = leaf.as<double>();
        } else {
            return;
        }
        if (!max_val || val > *max_val) {
            max_val = val;
        }
    });
    return max_val;
}

/**
 * @brief Find minimum numeric leaf value.
 *
 * @param view The root value to traverse
 * @return The minimum value, or nullopt if no numeric leaves
 */
inline std::optional<double> min_numeric(ConstValueView view) {
    std::optional<double> min_val;
    deep_visit(view, [&min_val](ConstValueView leaf, const TraversalPath&) {
        double val;
        if (leaf.is_scalar_type<int64_t>()) {
            val = static_cast<double>(leaf.as<int64_t>());
        } else if (leaf.is_scalar_type<double>()) {
            val = leaf.as<double>();
        } else {
            return;
        }
        if (!min_val || val < *min_val) {
            min_val = val;
        }
    });
    return min_val;
}

} // namespace hgraph::value
