#pragma once

/**
 * @file policy.h
 * @brief Policy-based extensions for the Value type system.
 *
 * Policies provide zero-overhead composition of value behaviors using
 * compile-time dispatch. The design follows the principle that unused
 * features should cost nothing.
 *
 * Available policies:
 * - NoCache: Default policy with no extensions (zero overhead)
 * - WithPythonCache: Caches Python object conversions
 *
 * The policy_traits template detects policy capabilities, and PolicyStorage
 * provides conditional storage that uses Empty Base Optimization (EBO) when
 * the policy requires no storage.
 */

#include <nanobind/nanobind.h>

#include <optional>
#include <type_traits>

namespace nb = nanobind;

namespace hgraph::value {

// ============================================================================
// Policy Tag Types
// ============================================================================

/**
 * @brief Default policy - no caching or extensions.
 *
 * This is an empty tag type that results in zero overhead.
 * Use this when you don't need Python object caching.
 */
struct NoCache {};

/**
 * @brief Policy that caches Python object conversions.
 *
 * When enabled, to_python() results are cached and reused until
 * the value is modified. This is useful when the same C++ value
 * will be converted to Python multiple times.
 */
struct WithPythonCache {};

// ============================================================================
// Policy Traits
// ============================================================================

/**
 * @brief Traits for detecting policy capabilities at compile time.
 *
 * Primary template - default capabilities (all false).
 */
template<typename Policy>
struct policy_traits {
    /// Whether this policy caches Python objects
    static constexpr bool has_python_cache = false;

    /// Whether this policy has any extensions that require storage
    static constexpr bool has_storage = false;
};

/**
 * @brief Specialization for WithPythonCache policy.
 */
template<>
struct policy_traits<WithPythonCache> {
    static constexpr bool has_python_cache = true;
    static constexpr bool has_storage = true;
};

// ============================================================================
// Policy Storage
// ============================================================================

/**
 * @brief Conditional storage based on policy - primary template (empty).
 *
 * This is the default case for policies that don't require storage.
 * Due to Empty Base Optimization (EBO), inheriting from this adds
 * zero bytes to the derived class.
 */
template<typename Policy, typename Enable>
struct PolicyStorage {
    // Empty - no storage needed
};

/**
 * @brief Policy storage specialization for Python caching.
 *
 * Provides storage for the cached Python object and methods for
 * cache management.
 */
template<typename Policy>
struct PolicyStorage<Policy, std::enable_if_t<policy_traits<Policy>::has_python_cache>> {
    /// Cached Python object (optional to allow invalidation)
    mutable std::optional<nb::object> _cached_python;

    /**
     * @brief Invalidate the cached Python object.
     *
     * Call this when the underlying value changes.
     */
    void invalidate_cache() const {
        _cached_python = std::nullopt;
    }

    /**
     * @brief Check if a cached Python object exists.
     * @return true if there's a valid cached object
     */
    [[nodiscard]] bool has_cache() const {
        return _cached_python.has_value();
    }

    /**
     * @brief Get the cached Python object.
     * @return The cached object, or nb::none() if not cached
     */
    [[nodiscard]] nb::object get_cache() const {
        return _cached_python ? *_cached_python : nb::none();
    }

    /**
     * @brief Set the cached Python object.
     * @param obj The object to cache
     */
    void set_cache(nb::object obj) const {
        _cached_python = std::move(obj);
    }
};

// ============================================================================
// Policy Helpers
// ============================================================================

/**
 * @brief Helper to check if a policy has Python caching.
 */
template<typename Policy>
inline constexpr bool has_python_cache_v = policy_traits<Policy>::has_python_cache;

/**
 * @brief Helper to check if a policy requires storage.
 */
template<typename Policy>
inline constexpr bool has_storage_v = policy_traits<Policy>::has_storage;

// ============================================================================
// Static Assertions
// ============================================================================

// Verify NoCache has no storage overhead
static_assert(sizeof(PolicyStorage<NoCache>) == 1,
    "Empty PolicyStorage should be 1 byte (for EBO)");

// Verify WithPythonCache has the expected storage
static_assert(policy_traits<WithPythonCache>::has_python_cache,
    "WithPythonCache should have python cache trait");

} // namespace hgraph::value
