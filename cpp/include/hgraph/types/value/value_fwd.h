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

/// Type operations vtable - function pointers for type-erased operations
struct TypeOps;

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

/// Non-owning const view into a Value
class ConstValueView;

/// Non-owning mutable view into a Value
class ValueView;

/// Const view with indexed access (base for Bundle, List)
class ConstIndexedView;

/// Mutable view with indexed access
class IndexedView;

/// Const view for tuples (heterogeneous, index-only)
class ConstTupleView;

/// Mutable view for tuples
class TupleView;

/// Const view for bundles (struct-like, named + indexed)
class ConstBundleView;

/// Mutable view for bundles
class BundleView;

/// Const view for lists (homogeneous indexed)
class ConstListView;

/// Mutable view for lists
class ListView;

/// Const view for sets (unique elements)
class ConstSetView;

/// Mutable view for sets
class SetView;

/// Const view for maps (key-value pairs)
class ConstMapView;

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
