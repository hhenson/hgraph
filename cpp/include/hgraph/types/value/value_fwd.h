#pragma once

/**
 * @file value_fwd.h
 * @brief Forward declarations for the Value type system.
 *
 * This header provides forward declarations of all Value-related types,
 * allowing headers to reference them without creating circular dependencies.
 */

#include <cstddef>
#include <cstdint>

namespace hgraph::value {

// ============================================================================
// Type System Forward Declarations
// ============================================================================

/// Type kind enumeration - identifies the category of a type
enum class TypeKind : uint8_t;

/// Type capability flags
enum class TypeFlags : uint32_t;

/// Type metadata structure - describes a type's layout and operations
struct TypeMeta;

/// Type operations — common ops + kind-tagged union of specific ops
struct type_ops;

/// Bundle field metadata
struct BundleFieldInfo;

// ============================================================================
// Storage Forward Declarations
// ============================================================================

/// Type-erased value storage with small buffer optimization
class ValueStorage;

// ============================================================================
// Policy Forward Declarations
// ============================================================================

/// Policy tag: no caching or extensions
struct NoCache;

/// Policy tag: Python object caching enabled
struct WithPythonCache;

/// Policy traits template - detect policy capabilities at compile time
template<typename Policy>
struct policy_traits;

/// Conditional storage based on policy - uses EBO for zero overhead
template<typename Policy, typename = void>
struct PolicyStorage;

// ============================================================================
// View Forward Declarations
// ============================================================================

/// Non-owning read-only view into a Value
class View;

/// Non-owning mutable view into a Value
class ValueView;

/// Mutable view with indexed access
class IndexedView;

/// Mutable view for tuples
class TupleView;

/// Mutable view for bundles
class BundleView;

/// Mutable view for lists
class ListView;

/// Mutable view for sets
class SetView;

/// Mutable view for maps
class MapView;

// ============================================================================
// Value Forward Declarations
// ============================================================================

/// Owning value storage with policy-based extensions
template<typename Policy = NoCache>
class Value;

/// Type aliases for common Value configurations
using PlainValue = Value<NoCache>;
using CachedValue = Value<WithPythonCache>;

// ============================================================================
// Type Registry Forward Declarations
// ============================================================================

/// Central registry for type metadata
class TypeRegistry;

/// Get the TypeMeta for a scalar type (convenience function)
template<typename T>
const TypeMeta* scalar_type_meta();

} // namespace hgraph::value
