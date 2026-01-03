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

#include <functional>
#include <optional>
#include <type_traits>
#include <vector>

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

/**
 * @brief Policy that tracks modifications and invokes callbacks.
 *
 * When enabled, callbacks can be registered via on_modified() and
 * will be invoked whenever the value changes.
 */
struct WithModificationTracking {};

/**
 * @brief Policy that validates values before accepting them.
 *
 * When enabled, from_python() will reject None values with an error.
 */
struct WithValidation {};

/**
 * @brief Combine multiple policies into one.
 *
 * Usage: Value<CombinedPolicy<WithPythonCache, WithModificationTracking>>
 */
template<typename... Policies>
struct CombinedPolicy {};

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

    /// Whether this policy tracks modifications
    static constexpr bool has_modification_tracking = false;

    /// Whether this policy validates input
    static constexpr bool has_validation = false;

    /// Whether this policy has any extensions that require storage
    static constexpr bool has_storage = false;
};

/**
 * @brief Specialization for WithPythonCache policy.
 */
template<>
struct policy_traits<WithPythonCache> {
    static constexpr bool has_python_cache = true;
    static constexpr bool has_modification_tracking = false;
    static constexpr bool has_validation = false;
    static constexpr bool has_storage = true;
};

/**
 * @brief Specialization for WithModificationTracking policy.
 */
template<>
struct policy_traits<WithModificationTracking> {
    static constexpr bool has_python_cache = false;
    static constexpr bool has_modification_tracking = true;
    static constexpr bool has_validation = false;
    static constexpr bool has_storage = true;
};

/**
 * @brief Specialization for WithValidation policy.
 */
template<>
struct policy_traits<WithValidation> {
    static constexpr bool has_python_cache = false;
    static constexpr bool has_modification_tracking = false;
    static constexpr bool has_validation = true;
    static constexpr bool has_storage = false;  // No storage needed - just validation logic
};

/**
 * @brief Specialization for CombinedPolicy - combines traits from all policies.
 */
template<typename... Policies>
struct policy_traits<CombinedPolicy<Policies...>> {
    static constexpr bool has_python_cache =
        (policy_traits<Policies>::has_python_cache || ...);
    static constexpr bool has_modification_tracking =
        (policy_traits<Policies>::has_modification_tracking || ...);
    static constexpr bool has_validation =
        (policy_traits<Policies>::has_validation || ...);
    static constexpr bool has_storage =
        (policy_traits<Policies>::has_storage || ...);
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
 * @brief Policy storage specialization for Python caching only (no modification tracking).
 *
 * Provides storage for the cached Python object and methods for
 * cache management.
 */
template<typename Policy>
struct PolicyStorage<Policy, std::enable_if_t<
    policy_traits<Policy>::has_python_cache &&
    !policy_traits<Policy>::has_modification_tracking>> {
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

    // Stub for modification tracking (no-op)
    void notify_modified() const {}
};

/**
 * @brief Policy storage specialization for modification tracking only (no caching).
 */
template<typename Policy>
struct PolicyStorage<Policy, std::enable_if_t<
    policy_traits<Policy>::has_modification_tracking &&
    !policy_traits<Policy>::has_python_cache>> {

    using callback_type = std::function<void()>;
    mutable std::vector<callback_type> _callbacks;

    void on_modified(callback_type cb) const {
        _callbacks.push_back(std::move(cb));
    }

    void notify_modified() const {
        for (auto& cb : _callbacks) {
            if (cb) cb();
        }
    }

    // Stub for caching (no-op)
    void invalidate_cache() const {}
};

/**
 * @brief Policy storage specialization for both Python caching and modification tracking.
 *
 * Used by TSValue = Value<CombinedPolicy<WithPythonCache, WithModificationTracking>>
 */
template<typename Policy>
struct PolicyStorage<Policy, std::enable_if_t<
    policy_traits<Policy>::has_python_cache &&
    policy_traits<Policy>::has_modification_tracking>> {

    // Cache storage
    mutable std::optional<nb::object> _cached_python;

    void invalidate_cache() const { _cached_python = std::nullopt; }
    [[nodiscard]] bool has_cache() const { return _cached_python.has_value(); }
    [[nodiscard]] nb::object get_cache() const {
        return _cached_python ? *_cached_python : nb::none();
    }
    void set_cache(nb::object obj) const { _cached_python = std::move(obj); }

    // Callback storage
    using callback_type = std::function<void()>;
    mutable std::vector<callback_type> _callbacks;

    void on_modified(callback_type cb) const {
        _callbacks.push_back(std::move(cb));
    }

    void notify_modified() const {
        invalidate_cache();  // Also invalidate cache on modification
        for (auto& cb : _callbacks) {
            if (cb) cb();
        }
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
 * @brief Helper to check if a policy has modification tracking.
 */
template<typename Policy>
inline constexpr bool has_modification_tracking_v = policy_traits<Policy>::has_modification_tracking;

/**
 * @brief Helper to check if a policy has validation.
 */
template<typename Policy>
inline constexpr bool has_validation_v = policy_traits<Policy>::has_validation;

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
