#ifndef HGRAPH_CPP_ROOT_MEMORY_UTILS_H
#define HGRAPH_CPP_ROOT_MEMORY_UTILS_H

#include <hgraph/config.h>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <hgraph/types/utils/intern_table.h>
#include <hgraph/util/scope.h>
#include <hgraph/util/tagged_ptr.h>

namespace hgraph
{
    /**
     * Foundation utilities for type-erased memory management.
     *
     * ``MemoryUtils`` is a non-instantiable struct used as a namespace for the
     * raw-memory primitives the runtime is built on:
     *
     * - ``StorageLayout`` and ``StoragePlan`` describe how to lay out, build
     *   and tear down a memory region.
     * - ``LifecycleOps`` is the type-erased ops table used to drive that
     *   lifecycle.
     * - ``AllocatorOps`` is the matching ops table for allocate / deallocate.
     * - ``CompositePlanBuilder`` and ``array_plan`` synthesise plans for
     *   composite and array layouts.
     * - ``ErasedOwner`` owns inline or allocated storage used by consumers
     *   such as ``Value``.
     * - ``plan_for<T>()`` and friends produce canonical plans for concrete
     *   C++ types.
     *
     * Every plan, composite layout, and array layout produced by this
     * namespace is interned via ``InternTable`` so equal layouts share a
     * single stable address.
     */
    struct MemoryUtils
    {
        /**
         * Byte size and alignment requirement for a piece of memory.
         *
         * ``alignment`` must be a power of two; ``size`` may be zero (used
         * for empty composite layouts and array plans with zero elements).
         */
        struct StorageLayout
        {
            /** Byte size of the layout. */
            size_t size{0};
            /** Required alignment in bytes; must be a power of two. */
            size_t alignment{1};

            /** True when ``alignment`` is non-zero and a power of two. */
            [[nodiscard]] constexpr bool valid() const noexcept { return alignment > 0 && std::has_single_bit(alignment); }
        };

        /**
         * Tag distinguishing the composite shapes ``MemoryUtils`` produces.
         */
        enum class CompositeKind : uint8_t {
            /** Plan has no composite structure (an atomic ``plan_for<T>``). */
            None,
            /** Index-addressed component layout. */
            Tuple,
            /** Name-addressed component layout. */
            NamedTuple,
            /** Repeated element layout. */
            Array,
        };

        struct StoragePlan;
        struct CompositeComponent;
        struct CompositeState;
        struct ArrayState;
        struct CompositePlanBuilder;
        struct AllocatorOps;

        /**
         * Type-erased lifecycle vtable carried by every ``StoragePlan``.
         *
         * Each function pointer takes the target memory as its first
         * argument and an optional ``context`` (the plan's
         * ``lifecycle_context``) as its last. ``nullptr`` means the
         * corresponding operation is not supported and the plan is treated
         * as missing that capability.
         */
        struct LifecycleOps
        {
            using construct_fn      = void (*)(void *, const void *);
            using destroy_fn        = void (*)(void *, const void *) noexcept;
            using copy_construct_fn = void (*)(void *, const void *, const void *);
            using move_construct_fn = void (*)(void *, void *, const void *);
            using copy_assign_fn    = void (*)(void *, const void *, const void *);
            using move_assign_fn    = void (*)(void *, void *, const void *);

            /** Default-construct an object at ``(memory, context)``. */
            construct_fn      construct{nullptr};
            /** Destroy the object at ``(memory, context)``. */
            destroy_fn        destroy{nullptr};
            /** Copy-construct an object at ``(dst, src, context)``. */
            copy_construct_fn copy_construct{nullptr};
            /** Move-construct an object at ``(dst, src, context)``. */
            move_construct_fn move_construct{nullptr};
            /** Copy-assign into an existing object at ``(dst, src, context)``. */
            copy_assign_fn    copy_assign{nullptr};
            /** Move-assign into an existing object at ``(dst, src, context)``. */
            move_assign_fn    move_assign{nullptr};

            /** True when ``construct`` is set. */
            [[nodiscard]] constexpr bool can_default_construct() const noexcept { return construct != nullptr; }
            /** True when ``destroy`` is set. */
            [[nodiscard]] constexpr bool can_destroy() const noexcept { return destroy != nullptr; }
            /** True when ``copy_construct`` is set. */
            [[nodiscard]] constexpr bool can_copy_construct() const noexcept { return copy_construct != nullptr; }
            /** True when ``move_construct`` is set. */
            [[nodiscard]] constexpr bool can_move_construct() const noexcept { return move_construct != nullptr; }
            /** True when ``copy_assign`` is set. */
            [[nodiscard]] constexpr bool can_copy_assign() const noexcept { return copy_assign != nullptr; }
            /** True when ``move_assign`` is set. */
            [[nodiscard]] constexpr bool can_move_assign() const noexcept { return move_assign != nullptr; }

            /** Invoke ``construct`` or throw if the hook is absent. */
            void default_construct_at(void *memory, const void *context = nullptr) const {
                if (construct == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a default-construction hook");
                }
                construct(memory, context);
            }

            /** Invoke ``destroy`` if the hook is set; tolerates a null memory pointer. */
            void destroy_at(void *memory, const void *context = nullptr) const noexcept {
                if (destroy != nullptr && memory != nullptr) { destroy(memory, context); }
            }

            /** Invoke ``copy_construct`` or throw if the hook is absent. */
            void copy_construct_at(void *dst, const void *src, const void *context = nullptr) const {
                if (copy_construct == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a copy-construction hook");
                }
                copy_construct(dst, src, context);
            }

            /** Invoke ``move_construct`` or throw if the hook is absent. */
            void move_construct_at(void *dst, void *src, const void *context = nullptr) const {
                if (move_construct == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a move-construction hook");
                }
                move_construct(dst, src, context);
            }

            /** Invoke ``copy_assign`` or throw if the hook is absent. */
            void copy_assign_at(void *dst, const void *src, const void *context = nullptr) const {
                if (copy_assign == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a copy-assignment hook");
                }
                copy_assign(dst, src, context);
            }

            /** Invoke ``move_assign`` or throw if the hook is absent. */
            void move_assign_at(void *dst, void *src, const void *context = nullptr) const {
                if (move_assign == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a move-assignment hook");
                }
                move_assign(dst, src, context);
            }
        };

        /**
         * Compile-time policy that controls when ``ErasedOwner`` chooses
         * inline (small-buffer) storage over heap allocation.
         *
         * Defaults to a pointer-sized inline buffer with pointer alignment.
         * Callers wanting a larger inline budget pass an explicit
         * specialisation, for example
         * ``InlineStoragePolicy<32, alignof(std::max_align_t)>``.
         */
        template <size_t InlineBytes = sizeof(void *), size_t InlineAlignment = alignof(void *)> struct InlineStoragePolicy
        {
            static_assert(InlineBytes > 0, "Inline storage must reserve at least one byte");
            static_assert(std::has_single_bit(InlineAlignment), "Inline storage alignment must be a power of two");

            /** Number of bytes reserved for inline storage. */
            static constexpr size_t inline_bytes     = InlineBytes;
            /** Required alignment of the inline storage region. */
            static constexpr size_t inline_alignment = InlineAlignment;

            /** Effective alignment used by ``ErasedOwner`` (at least pointer alignment). */
            [[nodiscard]] static constexpr size_t storage_alignment() noexcept {
                return InlineAlignment > alignof(void *) ? InlineAlignment : alignof(void *);
            }

            /**
             * True when a payload with ``layout`` can fit inline given the
             * supplied trivial-copy / trivial-destroy flags. Trivially
             * copyable and destructible payloads are required for inline
             * storage so move and assignment can use ``memcpy``.
             */
            [[nodiscard]] static constexpr bool can_store_inline(StorageLayout layout, bool trivially_copyable,
                                                                 bool trivially_destructible) noexcept {
                return layout.valid() && layout.size <= inline_bytes && layout.alignment <= inline_alignment &&
                       trivially_copyable && trivially_destructible;
            }
        };

        /**
         * Storage policy that always allocates the erased payload.
         *
         * Use this for implementation contexts whose address is published
         * through passive operation tables. Moving the owner transfers the
         * allocation and therefore cannot invalidate those published
         * context/result pointers.
         */
        struct HeapOnlyStoragePolicy
        {
            static constexpr size_t inline_bytes = sizeof(void *);

            [[nodiscard]] static constexpr size_t storage_alignment() noexcept { return alignof(void *); }

            [[nodiscard]] static constexpr bool can_store_inline(StorageLayout, bool, bool) noexcept
            {
                return false;
            }
        };

        /**
         * Description of one component inside a composite ``StoragePlan``.
         *
         * For tuples ``name`` is null and components are addressed by
         * ``index``. For named tuples ``name`` is the (interned) field name.
         */
        struct CompositeComponent
        {
            /** Position of this component within the composite. */
            size_t             index{0};
            /** Byte offset of this component from the start of the composite. */
            size_t             offset{0};
            /** Field name for named-tuple components; null for tuple components. */
            const char        *name{nullptr};
            /** Plan describing the layout and lifecycle of this component. */
            const StoragePlan *plan{nullptr};

            /** True when ``name`` is set (named-tuple component). */
            [[nodiscard]] bool is_named() const noexcept { return name != nullptr; }
        };

        /**
         * Trailing state attached to composite plans.
         *
         * Stored in the per-plan byte block referenced by
         * ``StoragePlan::lifecycle_context``. The component array immediately
         * follows the ``CompositeState`` header in memory.
         *
         * Note, this code could be significalntly simplified by using a flexible array member for the component array,
         * but that is not standard C++ as of 26 and would require compiler-specific GCC/Clang extensions and is not supported by MSVC.
         * Alternative is zero-size array member, but that is also not standard C++ even though supported by all compilers. Having -pendantic
         * will cause warnings.
         */
        struct CompositeState
        {
            /** Number of components in the composite. */
            size_t component_count{0};

            /** Byte offset from the start of the state block to the first component descriptor. */
            [[nodiscard]] static constexpr size_t components_offset() noexcept {
                static_assert(std::is_standard_layout_v<CompositeState>);
                static_assert(std::is_standard_layout_v<CompositeComponent>);
                struct Layout { CompositeState state; CompositeComponent first; };
                return offsetof(Layout, first);
            }

            /** Total state-block size needed to hold ``component_count`` components. */
            [[nodiscard]] static constexpr size_t storage_bytes(size_t component_count) noexcept {
                return components_offset() + sizeof(CompositeComponent) * component_count;
            }

            /** Mutable pointer to the in-place component array following this header. */
            [[nodiscard]] CompositeComponent *components() noexcept {
                auto *base = reinterpret_cast<std::byte *>(this) + components_offset();
                return std::launder(reinterpret_cast<CompositeComponent *>(base));
            }

            /** Const overload of ``components``. */
            [[nodiscard]] const CompositeComponent *components() const noexcept {
                auto *base = reinterpret_cast<const std::byte *>(this) + components_offset();
                return std::launder(reinterpret_cast<const CompositeComponent *>(base));
            }
        };

        /**
         * Trailing state for array plans.
         *
         * Carries the element plan, the element count, and the byte stride
         * used between elements in the array layout.
         */
        struct ArrayState
        {
            /** Plan describing each element. */
            const StoragePlan *element_plan{nullptr};
            /** Number of elements in the array. */
            size_t             element_count{0};
            /** Byte stride between consecutive elements. */
            size_t             element_stride{0};
        };

        /**
         * Plan describing how to allocate, construct, destruct, copy, and
         * move one piece of typed memory.
         *
         * A plan is the memory-layout primitive in the runtime: it carries a
         * ``StorageLayout``, a ``LifecycleOps`` table, an opaque
         * ``lifecycle_context`` (used by composite/array plans to point at
         * their state block), and a ``composite_kind_tag`` plus
         * trivially-* hints so consumers can pick fast paths.
         *
         * Plans are immutable once interned. Atomic plans come from
         * ``plan_for<T>()``, composite plans from ``CompositePlanBuilder``,
         * array plans from ``array_plan``.
         */
        struct StoragePlan
        {
            /** Byte size and alignment for the described memory. */
            StorageLayout layout{};
            /** Lifecycle hooks used to construct, destroy, copy, and move. */
            LifecycleOps  lifecycle{};
            /** Opaque per-plan context passed to every lifecycle hook. */
            const void   *lifecycle_context{nullptr};
            /** Composite shape this plan represents (``None`` for atomic). */
            CompositeKind composite_kind_tag{CompositeKind::None};
            /** True when the described type is trivially destructible. */
            bool          trivially_destructible{false};
            /** True when the described type is trivially copyable. */
            bool          trivially_copyable{false};
            /** True when the described type is trivially move-constructible. */
            bool          trivially_move_constructible{false};

            /**
             * True when the plan describes a meaningful piece of memory.
             * A valid layout plus at least one of: positive size, composite
             * structure, or a non-empty lifecycle hook.
             */
            [[nodiscard]] bool valid() const noexcept {
                return layout.valid() &&
                       (layout.size > 0 || composite_kind_tag != CompositeKind::None || lifecycle.can_default_construct() ||
                        lifecycle.can_copy_construct() || lifecycle.can_move_construct() || lifecycle.can_destroy());
            }

            /** Forward to ``lifecycle.can_default_construct``. */
            [[nodiscard]] constexpr bool can_default_construct() const noexcept { return lifecycle.can_default_construct(); }
            /** Forward to ``lifecycle.can_copy_construct``. */
            [[nodiscard]] constexpr bool can_copy_construct() const noexcept { return lifecycle.can_copy_construct(); }
            /** Forward to ``lifecycle.can_move_construct``. */
            [[nodiscard]] constexpr bool can_move_construct() const noexcept { return lifecycle.can_move_construct(); }
            /** Forward to ``lifecycle.can_copy_assign``. */
            [[nodiscard]] constexpr bool can_copy_assign() const noexcept { return lifecycle.can_copy_assign(); }
            /** Forward to ``lifecycle.can_move_assign``. */
            [[nodiscard]] constexpr bool can_move_assign() const noexcept { return lifecycle.can_move_assign(); }
            /** True when destruction is required (non-trivial and the hook is present). */
            [[nodiscard]] constexpr bool requires_destroy() const noexcept {
                return !trivially_destructible && lifecycle.can_destroy();
            }

            /** True when this plan is a tuple or a named tuple. */
            [[nodiscard]] bool is_composite() const noexcept {
                return composite_kind_tag == CompositeKind::Tuple || composite_kind_tag == CompositeKind::NamedTuple;
            }
            /** True when this plan is a (positional) tuple. */
            [[nodiscard]] bool          is_tuple() const noexcept { return composite_kind_tag == CompositeKind::Tuple; }
            /** True when this plan is a named tuple. */
            [[nodiscard]] bool          is_named_tuple() const noexcept { return composite_kind_tag == CompositeKind::NamedTuple; }
            /** True when this plan is an array. */
            [[nodiscard]] bool          is_array() const noexcept { return composite_kind_tag == CompositeKind::Array; }
            /** Composite kind tag; throws if the plan has no composite structure. */
            [[nodiscard]] CompositeKind composite_kind() const {
                if (composite_kind_tag == CompositeKind::None) {
                    throw std::logic_error("MemoryUtils::StoragePlan is not structured");
                }
                return composite_kind_tag;
            }
            /** Pointer to the trailing composite state, or null for non-composite plans. */
            [[nodiscard]] const CompositeState *composite_state() const noexcept {
                return is_composite() ? static_cast<const CompositeState *>(lifecycle_context) : nullptr;
            }
            /** Pointer to the trailing array state, or null for non-array plans. */
            [[nodiscard]] const ArrayState *array_state() const noexcept {
                return is_array() ? static_cast<const ArrayState *>(lifecycle_context) : nullptr;
            }
            /** Number of components in a composite; zero for non-composites. */
            [[nodiscard]] size_t component_count() const noexcept {
                return composite_state() ? composite_state()->component_count : 0;
            }
            /** Element count of an array plan; zero for non-arrays. */
            [[nodiscard]] size_t array_count() const noexcept { return array_state() ? array_state()->element_count : 0; }
            /** Byte stride between elements of an array plan; zero for non-arrays. */
            [[nodiscard]] size_t array_stride() const noexcept { return array_state() ? array_state()->element_stride : 0; }
            /** Element plan for an array; throws if this is not an array plan. */
            [[nodiscard]] const StoragePlan &array_element_plan() const {
                const ArrayState *state = array_state();
                if (state == nullptr || state->element_plan == nullptr) {
                    throw std::logic_error("MemoryUtils::StoragePlan is not an array");
                }
                return *state->element_plan;
            }
            /** Byte offset of array element ``index``; throws if out of range or not an array. */
            [[nodiscard]] size_t element_offset(size_t index) const {
                const ArrayState *state = array_state();
                if (state == nullptr || index >= state->element_count) {
                    throw std::out_of_range("MemoryUtils::StoragePlan array index out of range");
                }
                return index * state->element_stride;
            }
            /** Component at ``index``; throws when this is not a composite or the index is out of range. */
            [[nodiscard]] const CompositeComponent &component(size_t index) const {
                const CompositeState *state = composite_state();
                if (state == nullptr || index >= state->component_count) {
                    throw std::out_of_range("MemoryUtils::StoragePlan component index out of range");
                }
                return state->components()[index];
            }
            /** Lookup a named-tuple component by name; returns null when missing or when this is not a named tuple. */
            [[nodiscard]] const CompositeComponent *find_component(std::string_view name) const noexcept {
                if (!is_named_tuple()) { return nullptr; }
                const CompositeState *state = composite_state();
                for (size_t index = 0; index < state->component_count; ++index) {
                    const CompositeComponent &component = state->components()[index];
                    if (component.name != nullptr && name == component.name) { return &component; }
                }
                return nullptr;
            }
            /** Like ``find_component`` but throws ``std::out_of_range`` when the field is missing. */
            [[nodiscard]] const CompositeComponent &component(std::string_view name) const {
                if (const CompositeComponent *result = find_component(name); result != nullptr) { return *result; }
                throw std::out_of_range("MemoryUtils::StoragePlan field not found");
            }
            /** Span over the component array; empty for non-composites. */
            [[nodiscard]] std::span<const CompositeComponent> components() const noexcept {
                const CompositeState *state = composite_state();
                return state ? std::span<const CompositeComponent>(state->components(), state->component_count)
                             : std::span<const CompositeComponent>{};
            }

            /** True when this plan can be stored inline under ``Policy``. */
            template <typename Policy = InlineStoragePolicy<>> [[nodiscard]] constexpr bool stores_inline() const noexcept {
                return Policy::can_store_inline(layout, trivially_copyable, trivially_destructible);
            }

            /** True when storage allocated under ``Policy`` will need a matching ``deallocate``. */
            template <typename Policy = InlineStoragePolicy<>> [[nodiscard]] constexpr bool requires_deallocate() const noexcept {
                return !stores_inline<Policy>();
            }

            /** Default-construct an object at ``memory`` using the bound lifecycle hooks. */
            void default_construct(void *memory) const { lifecycle.default_construct_at(memory, lifecycle_context); }

            /** Destroy the object at ``memory`` if a destroy hook is set. */
            void destroy(void *memory) const noexcept { lifecycle.destroy_at(memory, lifecycle_context); }

            /** Copy-construct ``dst`` from ``src`` using the bound lifecycle hooks. */
            void copy_construct(void *dst, const void *src) const { lifecycle.copy_construct_at(dst, src, lifecycle_context); }

            /** Move-construct ``dst`` from ``src`` using the bound lifecycle hooks. */
            void move_construct(void *dst, void *src) const { lifecycle.move_construct_at(dst, src, lifecycle_context); }

            /** Copy-assign ``src`` into ``dst`` using the bound lifecycle hooks. */
            void copy_assign(void *dst, const void *src) const { lifecycle.copy_assign_at(dst, src, lifecycle_context); }

            /** Move-assign ``src`` into ``dst`` using the bound lifecycle hooks. */
            void move_assign(void *dst, void *src) const { lifecycle.move_assign_at(dst, src, lifecycle_context); }
        };

        /**
         * Type-erased ops table for raw heap allocation and deallocation.
         *
         * Defaults to the runtime's aligned-new / aligned-delete pair.
         * Custom allocators provide their own functions and may carry
         * additional state by capturing it in the function pointers
         * themselves.
         */
        struct AllocatorOps
        {
            using allocate_fn   = void *(*)(StorageLayout);
            using deallocate_fn = void (*)(void *, StorageLayout) noexcept;

            /** Allocation hook. */
            allocate_fn   allocate{&MemoryUtils::default_allocate};
            /** Deallocation hook. */
            deallocate_fn deallocate{&MemoryUtils::default_deallocate};

            /** Allocate a block matching ``layout``; throws if no hook is configured. */
            [[nodiscard]] void *allocate_storage(StorageLayout layout) const {
                if (allocate == nullptr) { throw std::logic_error("MemoryUtils::AllocatorOps is missing an allocation hook"); }
                return allocate(layout);
            }

            /** Deallocate a block previously returned by ``allocate_storage``. */
            void deallocate_storage(void *memory, StorageLayout layout) const noexcept {
                if (deallocate != nullptr && memory != nullptr) { deallocate(memory, layout); }
            }
        };

        /** Process-wide default allocator (aligned ``new`` / ``delete``). */
        [[nodiscard]] static const AllocatorOps &allocator() noexcept {
            static const AllocatorOps allocator_ops{};
            return allocator_ops;
        }

        /**
         * Concept satisfied by binding types that expose a ``StoragePlan``
         * via ``plan()`` / ``checked_plan()``. Used by ``ErasedOwner`` to
         * accept either a raw plan or a richer binding type.
         */
        template <typename Binding>
        [[nodiscard]] static constexpr const StoragePlan *storage_binding_plan(const Binding &binding) noexcept
        {
            if constexpr (requires { binding.plan(); }) return binding.plan();
            else return binding.plan;
        }

        template <typename Binding>
        [[nodiscard]] static const StoragePlan &checked_storage_binding_plan(const Binding &binding)
        {
            const auto *plan = storage_binding_plan(binding);
            if (plan == nullptr) throw std::logic_error("storage identity has no plan");
            return *plan;
        }

        template <typename Binding>
        static constexpr bool storage_binding = requires(const Binding &binding) {
            { storage_binding_plan(binding) } -> std::same_as<const StoragePlan *>;
        };

        /**
         * Fluent builder that accumulates components for a composite plan
         * and interns the result.
         *
         * Use ``MemoryUtils::tuple()`` for positional tuples and
         * ``MemoryUtils::named_tuple()`` for name-addressed bundles. Calling
         * ``add_field`` on a tuple builder (or ``add_plan`` on a named-tuple
         * builder) throws.
         */
        struct CompositePlanBuilder
        {
            /**
             * One pending component held by the builder before
             * ``build()`` is called. Once the plan is interned the
             * components live inside the plan's ``CompositeState`` block.
             */
            struct PendingComponent
            {
                /** Field name (empty for tuple components). */
                std::string        name{};
                /** Plan describing this component. */
                const StoragePlan *plan{nullptr};
            };

            /** Construct a builder for ``kind``; prefer the named factories. */
            explicit CompositePlanBuilder(CompositeKind kind) noexcept : m_kind(kind) {}

            /** Reserve component capacity to avoid intermediate reallocations. */
            CompositePlanBuilder &reserve(size_t count) {
                m_components.reserve(count);
                return *this;
            }

            /** Append an unnamed component using ``plan``; only valid for tuple builders. */
            CompositePlanBuilder &add_plan(const StoragePlan &plan) {
                ensure_kind(CompositeKind::Tuple, "add_plan");
                add_pending_component({}, plan);
                return *this;
            }

            /** Append a tuple component using the canonical plan for ``T``. */
            template <typename T> CompositePlanBuilder &add_type() { return add_plan(MemoryUtils::plan_for<T>()); }

            /**
             * Append a named component; only valid for named-tuple builders.
             * Names must be non-empty and unique within the builder.
             */
            CompositePlanBuilder &add_field(std::string_view name, const StoragePlan &plan) {
                ensure_kind(CompositeKind::NamedTuple, "add_field");
                if (name.empty()) { throw std::logic_error("MemoryUtils::CompositePlanBuilder field names must not be empty"); }
                if (has_field(name)) { throw std::logic_error("MemoryUtils::CompositePlanBuilder field names must be unique"); }
                add_pending_component(std::string(name), plan);
                return *this;
            }

            /** Append a named component using the canonical plan for ``T``. */
            template <typename T> CompositePlanBuilder &add_field(std::string_view name) {
                return add_field(name, MemoryUtils::plan_for<T>());
            }

            /**
             * Append an unnamed implementation component to a named-tuple plan.
             *
             * This is for schema-owned auxiliary storage, such as bundle field
             * validity. The component participates in lifecycle/copy layout, but
             * is intentionally not discoverable through ``find_component``.
             */
            CompositePlanBuilder &add_hidden_plan(const StoragePlan &plan) {
                // A trailing UNNAMED component (field-validity words) - valid
                // for both tuple and named-tuple builders.
                add_pending_component({}, plan);
                return *this;
            }

            /**
             * Synthesise and intern the composite plan described by the
             * accumulated components. Equivalent calls always return the
             * same canonical plan address.
             */
            [[nodiscard]] const StoragePlan &build() const { return composite_registry().intern(m_kind, m_components); }

          private:
            CompositeKind                 m_kind{CompositeKind::Tuple};
            std::vector<PendingComponent> m_components{};

            void ensure_kind(CompositeKind expected, std::string_view action) const {
                if (m_kind != expected) {
                    throw std::logic_error(std::string("MemoryUtils::CompositePlanBuilder::") + std::string(action) +
                                           (expected == CompositeKind::Tuple ? " is only valid for tuple builders"
                                                                             : " is only valid for named tuple builders"));
                }
            }

            [[nodiscard]] bool has_field(std::string_view name) const noexcept {
                return std::ranges::any_of(m_components,
                                           [name](const PendingComponent &component) { return component.name == name; });
            }

            void add_pending_component(std::string name, const StoragePlan &plan) {
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::CompositePlanBuilder requires valid child plans"); }
                m_components.push_back(PendingComponent{
                    .name = std::move(name),
                    .plan = &plan,
                });
            }
        };

        /**
         * Type-erased owner for a piece of memory described by a
         * ``StoragePlan`` (or a richer ``Binding`` wrapper).
         *
         * Two live states are supported:
         *
         * - ``OwningInline``: payload lives in the inline buffer when the
         *   plan fits under ``Policy``.
         * - ``OwningHeap``: payload was heap-allocated through the bound
         *   ``AllocatorOps``.
         * The default state is empty (``has_value()`` returns false). The
         * underlying storage uses a tagged-pointer-encoded state to keep
         * the owner three pointers wide with a pointer-sized inline payload.
         *
         * Template parameters:
         * - ``Policy``  ``InlineStoragePolicy`` controlling the inline
         *               buffer size and alignment.
         * - ``Binding`` either ``void`` (use a raw ``StoragePlan``) or a
         *               type satisfying ``storage_binding`` that exposes
         *               the plan via ``plan()`` / ``checked_plan()``.
         */
        template <typename Policy = InlineStoragePolicy<>, typename Binding = void>
            requires(std::is_void_v<Binding> || storage_binding<Binding>)
        class ErasedOwner
        {
          public:
            /** Construct an empty owner. */
            ErasedOwner() = default;

            /** Build a default-constructed payload from ``plan`` (raw-plan overload). */
            explicit ErasedOwner(const StoragePlan &plan, const AllocatorOps &allocator = MemoryUtils::allocator())
                requires std::is_void_v<Binding>
            {
                construct_owned_default(plan, allocator);
            }

            /** Build a default-constructed payload from ``binding`` (binding overload). */
            template <typename B = Binding>
                requires(!std::is_void_v<B>)
            explicit ErasedOwner(const B &binding, const AllocatorOps &allocator = MemoryUtils::allocator()) {
                construct_owned_default(binding, allocator);
            }

            /** Copy construction allocates fresh storage and copy-constructs the payload. */
            ErasedOwner(const ErasedOwner &other) {
                if (!other.has_value()) {
                    m_identity = other.m_identity;
                    return;
                }

                if constexpr (std::is_void_v<Binding>) {
                    construct_owned_copy(*other.plan(), other.data(), *other.allocator());
                } else {
                    construct_owned_copy(*other.binding(), other.data(), *other.allocator());
                }
            }

            /** Move construction transfers ownership of the payload. */
            ErasedOwner(ErasedOwner &&other) noexcept { move_from(std::move(other)); }

            /** Copy assignment resets the existing payload then copy-constructs from ``other``. */
            ErasedOwner &operator=(const ErasedOwner &other) {
                if (this != &other) {
                    reset();
                    if (other.has_value()) {
                        if constexpr (std::is_void_v<Binding>) {
                            construct_owned_copy(*other.plan(), other.data(), *other.allocator());
                        } else {
                            construct_owned_copy(*other.binding(), other.data(), *other.allocator());
                        }
                    } else {
                        m_identity = other.m_identity;
                    }
                }
                return *this;
            }

            /** Move assignment resets the existing payload then adopts ``other``. */
            ErasedOwner &operator=(ErasedOwner &&other) noexcept {
                if (this != &other) {
                    reset();
                    move_from(std::move(other));
                }
                return *this;
            }

            /** Destructor calls ``reset`` so any owned payload is destroyed and (if heap) deallocated. */
            ~ErasedOwner() { reset(); }

            /** Factory: default-construct an owner from ``plan``. */
            [[nodiscard]] static ErasedOwner owning(const StoragePlan  &plan,
                                                      const AllocatorOps &allocator = MemoryUtils::allocator())
                requires std::is_void_v<Binding>
            {
                return ErasedOwner(plan, allocator);
            }

            /** Factory: default-construct an owner from ``binding``. */
            template <typename B = Binding>
                requires(!std::is_void_v<B>)
            [[nodiscard]] static ErasedOwner owning(const B &binding, const AllocatorOps &allocator = MemoryUtils::allocator()) {
                return ErasedOwner(binding, allocator);
            }

            /** Factory: create an empty owner that still retains a bound plan. */
            [[nodiscard]] static ErasedOwner empty(const StoragePlan &plan)
                requires std::is_void_v<Binding>
            {
                ErasedOwner owner;
                owner.m_identity = &plan;
                return owner;
            }

            /** Factory: create an empty owner that still retains a bound binding. */
            template <typename B = Binding>
                requires(!std::is_void_v<B>)
            [[nodiscard]] static ErasedOwner empty(const B &binding) {
                ErasedOwner owner;
                owner.m_identity = &binding;
                return owner;
            }

            /** Factory: copy-construct an owner from ``src`` using ``plan``. */
            [[nodiscard]] static ErasedOwner owning_copy(const StoragePlan &plan, const void *src,
                                                           const AllocatorOps &allocator = MemoryUtils::allocator())
                requires std::is_void_v<Binding>
            {
                ErasedOwner owner;
                owner.construct_owned_copy(plan, src, allocator);
                return owner;
            }

            /**
             * Factory: allocate owning storage for a raw ``plan`` and let
             * the caller construct the payload in-place.
             *
             * This is the explicit ownership path for non-default-
             * constructible implementation contexts. The plan must describe
             * the concrete object actually constructed by ``construct`` so
             * its destruction hook remains exact after type erasure.
             */
            template <typename Construct>
            [[nodiscard]] static ErasedOwner owning_constructed(
                const StoragePlan &plan,
                Construct &&construct,
                const AllocatorOps &allocator = MemoryUtils::allocator())
                requires std::is_void_v<Binding>
            {
                ErasedOwner owner;
                owner.construct_owned_custom(plan, std::forward<Construct>(construct), allocator);
                return owner;
            }

            /** Factory: copy-construct an owner from ``src`` using ``binding``. */
            template <typename B = Binding>
                requires(!std::is_void_v<B>)
            [[nodiscard]] static ErasedOwner owning_copy(const B &binding, const void *src,
                                                           const AllocatorOps &allocator = MemoryUtils::allocator()) {
                ErasedOwner owner;
                owner.construct_owned_copy(binding, src, allocator);
                return owner;
            }

            /**
             * Factory: allocate owning storage for ``binding`` and let the
             * caller construct the payload in-place.
             *
             * ``construct`` is invoked exactly once with the destination
             * memory pointer. If it throws, any heap allocation is released
             * and the returned owner is not produced.
             */
            template <typename Construct, typename B = Binding>
                requires(!std::is_void_v<B>)
            [[nodiscard]] static ErasedOwner owning_constructed(
                const B &binding,
                Construct &&construct,
                const AllocatorOps &allocator = MemoryUtils::allocator())
            {
                ErasedOwner owner;
                owner.construct_owned_custom(binding, std::forward<Construct>(construct), allocator);
                return owner;
            }

            /** True when the owner holds a payload. */
            [[nodiscard]] bool     has_value() const noexcept {
                const State state = storage_state();
                return state == State::OwningInline || state == State::OwningHeap;
            }
            [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
            /** Every live owner owns its payload, either inline or on the heap. */
            [[nodiscard]] bool     is_owning() const noexcept { return has_value(); }
            /** True when the owned payload is held in inline storage. */
            [[nodiscard]] bool               stores_inline() const noexcept { return storage_state() == State::OwningInline; }
            /** True when the owned payload is held on the heap. */
            [[nodiscard]] bool               stores_heap() const noexcept { return storage_state() == State::OwningHeap; }
            /** Bound storage plan, or ``nullptr`` if the owner has no retained identity. */
            [[nodiscard]] const StoragePlan *plan() const noexcept {
                if constexpr (std::is_void_v<Binding>) {
                    return m_identity;
                } else {
                    return m_identity != nullptr ? storage_binding_plan(*m_identity) : nullptr;
                }
            }
            /** Bound binding (when ``Binding`` is not ``void``); ``nullptr`` if no identity is retained. */
            [[nodiscard]] const Binding *binding() const noexcept
                requires(!std::is_void_v<Binding>)
            {
                return m_identity;
            }
            /** Allocator used to back the payload. */
            [[nodiscard]] const AllocatorOps *allocator() const noexcept { return tagged_allocator(); }

            /** Mutable byte pointer to the payload, or ``nullptr`` when empty. */
            [[nodiscard]] void *data() noexcept {
                switch (storage_state()) {
                    case State::OwningInline: return static_cast<void *>(m_storage.inline_bytes.data());
                    case State::OwningHeap: return m_storage.ptr;
                    default: return nullptr;
                }
            }

            /** Const byte pointer to the payload, or ``nullptr`` when empty. */
            [[nodiscard]] const void *data() const noexcept {
                switch (storage_state()) {
                    case State::OwningInline: return static_cast<const void *>(m_storage.inline_bytes.data());
                    case State::OwningHeap: return m_storage.ptr;
                    default: return nullptr;
                }
            }

            /** Stable offsets consumed by the data-only debugger owner protocol. */
            [[nodiscard]] static constexpr size_t debug_identity_offset() noexcept {
                return offsetof(ErasedOwner, m_identity);
            }
            [[nodiscard]] static constexpr size_t debug_state_offset() noexcept {
                return offsetof(ErasedOwner, m_allocator_state);
            }
            [[nodiscard]] static constexpr size_t debug_storage_offset() noexcept {
                return offsetof(ErasedOwner, m_storage);
            }

            /** Typed pointer to the payload via ``MemoryUtils::cast<T>``. */
            template <typename T> [[nodiscard]] T *as() noexcept { return MemoryUtils::cast<T>(data()); }

            /** Const overload of ``as``. */
            template <typename T> [[nodiscard]] const T *as() const noexcept { return MemoryUtils::cast<T>(data()); }

            /**
             * Make a copy of this owner, copy-constructing a fresh
             * payload. Empty-but-bound owners clone to the same retained
             * identity without constructing a payload.
             */
            [[nodiscard]] ErasedOwner clone() const {
                if (!has_value()) {
                    ErasedOwner owner;
                    owner.m_identity = m_identity;
                    return owner;
                }

                if constexpr (std::is_void_v<Binding>) {
                    return owning_copy(*plan(), data(), *allocator());
                } else {
                    return owning_copy(*binding(), data(), *allocator());
                }
            }

            /**
             * Rebuild this owner to the default value for its bound plan.
             *
             * The replacement owner is constructed first so a throwing default
             * constructor leaves the original owner unchanged.
             */
            void reset_to_default() {
                const State state = storage_state();
                if (plan() == nullptr) {
                    throw std::logic_error("MemoryUtils::ErasedOwner::reset_to_default requires a bound plan");
                }
                if (state != State::OwningInline && state != State::OwningHeap) {
                    throw std::logic_error("MemoryUtils::ErasedOwner::reset_to_default requires a live owner");
                }

                ErasedOwner replacement = [&]() {
                    if constexpr (std::is_void_v<Binding>) {
                        return ErasedOwner(*plan(), *allocator());
                    } else {
                        return ErasedOwner(*binding(), *allocator());
                    }
                }();
                *this = std::move(replacement);
            }

            /**
             * Destroy and (if heap-allocated) deallocate the held payload,
             * leaving the owner empty.
             */
            void reset() noexcept { reset_impl(false); }

            /**
             * Destroy and release the held payload while preserving the
             * bound plan / binding identity. This is the owner
             * primitive for typed-null values.
             */
            void reset_payload() noexcept { reset_impl(true); }

          private:
            void reset_impl(bool preserve_identity) noexcept {
                auto       *identity_before = m_identity;
                const State state        = storage_state();
                const auto *storage_plan = plan();
                if (state == State::OwningInline || state == State::OwningHeap) {
                    storage_plan->destroy(data());
                    if (state == State::OwningHeap) {
                        allocator()->deallocate_storage(m_storage.ptr, storage_plan->layout);
                        m_storage.ptr = nullptr;
                    }
                }

                m_identity = preserve_identity ? identity_before : nullptr;
                m_allocator_state.clear();
            }

            enum class State : uint8_t {
                Empty,
                OwningInline,
                OwningHeap,
                Reserved,
            };

            union alignas(Policy::storage_alignment()) Storage {
                std::array<std::byte, Policy::inline_bytes> inline_bytes;
                void                                       *ptr;

                constexpr Storage() noexcept : ptr(nullptr) {}
            };

            using allocator_state_ptr = ::hgraph::tagged_ptr<const AllocatorOps, 2, State>;

            using identity_ptr = std::conditional_t<std::is_void_v<Binding>, const StoragePlan *, const Binding *>;

            identity_ptr        m_identity{nullptr};
            allocator_state_ptr m_allocator_state{};
            Storage             m_storage{};

            [[nodiscard]] const AllocatorOps *tagged_allocator() const noexcept { return m_allocator_state.ptr(); }

            [[nodiscard]] State storage_state() const noexcept { return m_allocator_state.enum_value(); }

            void set_allocator_state(const AllocatorOps *allocator, State state) noexcept {
                m_allocator_state.set(allocator, state);
            }

            [[nodiscard]] static State owning_state_for(const StoragePlan &plan) noexcept {
                return plan.template stores_inline<Policy>() ? State::OwningInline : State::OwningHeap;
            }

            void abandon_failed_construction() noexcept {
                if (storage_state() == State::OwningHeap) {
                    tagged_allocator()->deallocate_storage(m_storage.ptr, plan()->layout);
                    m_storage.ptr = nullptr;
                }
                m_identity = nullptr;
                m_allocator_state.clear();
            }

            void construct_owned_default(const StoragePlan &plan, const AllocatorOps &allocator) {
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::ErasedOwner requires a valid plan"); }

                m_identity = &plan;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(plan.layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    plan.default_construct(data());
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                plan.default_construct(data());
                rollback.release();
            }

            template <typename B = Binding>
                requires(!std::is_void_v<B>)
            void construct_owned_default(const B &binding, const AllocatorOps &allocator) {
                const StoragePlan &plan = checked_storage_binding_plan(binding);
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::ErasedOwner requires a valid plan"); }

                m_identity = &binding;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(plan.layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    plan.default_construct(data());
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                plan.default_construct(data());
                rollback.release();
            }

            void construct_owned_copy(const StoragePlan &plan, const void *src, const AllocatorOps &allocator) {
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::ErasedOwner requires a valid plan"); }

                m_identity = &plan;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(plan.layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    plan.copy_construct(data(), src);
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                plan.copy_construct(data(), src);
                rollback.release();
            }

            template <typename Construct>
            void construct_owned_custom(const StoragePlan &plan,
                                        Construct &&construct,
                                        const AllocatorOps &allocator)
            {
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::ErasedOwner requires a valid plan"); }

                m_identity = &plan;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(plan.layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    std::forward<Construct>(construct)(data());
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                std::forward<Construct>(construct)(data());
                rollback.release();
            }

            template <typename B = Binding>
                requires(!std::is_void_v<B>)
            void construct_owned_copy(const B &binding, const void *src, const AllocatorOps &allocator) {
                const StoragePlan &plan = checked_storage_binding_plan(binding);
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::ErasedOwner requires a valid plan"); }

                m_identity = &binding;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(plan.layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    plan.copy_construct(data(), src);
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                plan.copy_construct(data(), src);
                rollback.release();
            }

            template <typename B, typename Construct>
                requires(!std::is_void_v<B>)
            void construct_owned_custom(const B &binding, Construct &&construct, const AllocatorOps &allocator) {
                const StoragePlan &plan = checked_storage_binding_plan(binding);
                if (!plan.valid()) { throw std::logic_error("MemoryUtils::ErasedOwner requires a valid plan"); }

                m_identity = &binding;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(plan.layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    std::forward<Construct>(construct)(data());
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                std::forward<Construct>(construct)(data());
                rollback.release();
            }

            void move_from(ErasedOwner &&other) noexcept {
                m_identity        = std::exchange(other.m_identity, nullptr);
                m_allocator_state = std::exchange(other.m_allocator_state, allocator_state_ptr{});

                switch (storage_state()) {
                    case State::OwningInline:
                        std::memcpy(m_storage.inline_bytes.data(), other.m_storage.inline_bytes.data(), Policy::inline_bytes);
                        break;
                    case State::OwningHeap: m_storage.ptr = std::exchange(other.m_storage.ptr, nullptr); break;
                    default: m_storage.ptr = nullptr; break;
                }
            }
        };

        /** Begin building a positional tuple plan. */
        [[nodiscard]] static CompositePlanBuilder tuple() { return CompositePlanBuilder{CompositeKind::Tuple}; }

        /** Begin building a named-tuple plan. */
        [[nodiscard]] static CompositePlanBuilder named_tuple() { return CompositePlanBuilder{CompositeKind::NamedTuple}; }

        /** Alias for ``tuple()`` for callers that prefer a generic name. */
        [[nodiscard]] static CompositePlanBuilder composite() { return tuple(); }

        /** Convenience: intern a tuple plan from a list of component plans. */
        [[nodiscard]] static const StoragePlan &tuple_plan(std::initializer_list<const StoragePlan *> components) {
            auto builder = tuple();
            builder.reserve(components.size());
            for (const StoragePlan *plan : components) {
                if (plan == nullptr) { throw std::logic_error("MemoryUtils::tuple_plan requires non-null child plans"); }
                builder.add_plan(*plan);
            }
            return builder.build();
        }

        /** Convenience: intern a named-tuple plan from a list of ``(name, plan)`` pairs. */
        [[nodiscard]] static const StoragePlan &
        named_tuple_plan(std::initializer_list<std::pair<std::string_view, const StoragePlan *>> components) {
            auto builder = named_tuple();
            builder.reserve(components.size());
            for (const auto &[name, plan] : components) {
                if (plan == nullptr) { throw std::logic_error("MemoryUtils::named_tuple_plan requires non-null child plans"); }
                builder.add_field(name, *plan);
            }
            return builder.build();
        }

        /** Alias for ``tuple_plan`` for callers that prefer a generic name. */
        [[nodiscard]] static const StoragePlan &composite_plan(std::initializer_list<const StoragePlan *> components) {
            return tuple_plan(components);
        }

        /** Intern an array plan describing ``count`` elements laid out under ``element_plan``. */
        [[nodiscard]] static const StoragePlan &array_plan(const StoragePlan &element_plan, size_t count) {
            if (!element_plan.valid()) { throw std::logic_error("MemoryUtils::array_plan requires a valid element plan"); }
            return array_registry().intern(element_plan, count);
        }

        /**
         * Intern an uninitialised byte-storage plan for ``layout``.
         *
         * Raw plans participate in composite node/graph layouts but own no
         * object lifecycle. Default construction is therefore a no-op; the
         * runtime owner explicitly constructs and destroys any payload placed
         * in the region. This is used for caller-owned nested-graph storage.
         */
        [[nodiscard]] static const StoragePlan &raw_storage_plan(StorageLayout layout) {
            if (!layout.valid() || layout.size == 0) {
                throw std::logic_error("MemoryUtils::raw_storage_plan requires a non-empty valid layout");
            }
            return raw_plan_registry().intern(layout);
        }

        /** Typed convenience: intern ``array_plan(plan_for<T>(), count)``. */
        template <typename T> [[nodiscard]] static const StoragePlan &array_plan(size_t count) {
            return array_plan(plan_for<T>(), count);
        }

        /**
         * Clear the synthesised composite, array, and raw-storage plan registries.
         * Composite and array plans intern by child/element-plan **pointer**, so
         * they hold dangling keys once the underlying plans are torn down. Any
         * reset path that frees plans (e.g. a registry reset between tests) must
         * call this, or a later plan that reuses a freed address will get a stale
         * composite/array plan — producing a wrong layout (overlapping fields)
         * and memory corruption. Plans are program-lifetime in normal use, so
         * this is only exercised on reset.
         */
        static void clear_synthesised_plans() noexcept {
            composite_registry().clear();
            array_registry().clear();
            raw_plan_registry().clear();
        }

        /**
         * Canonical atomic plan for ``T``.
         *
         * The plan is a function-local ``static`` so the same address is
         * returned for every call with the same ``T``. Lifecycle hooks are
         * elided for trivially-destructible / trivially-copyable types.
         */
        template <typename T>
        [[nodiscard]] static HGRAPH_NOINLINE const StoragePlan &plan_for() noexcept {
            using Type                    = std::remove_cv_t<std::remove_reference_t<T>>;
            static const StoragePlan plan = {
                .layout = layout_for<Type>(),
                .lifecycle =
                    {
                        .construct      = lifecycle_construct<Type>(),
                        .destroy        = lifecycle_destroy<Type>(),
                        .copy_construct = lifecycle_copy_construct<Type>(),
                        .move_construct = lifecycle_move_construct<Type>(),
                        .copy_assign    = lifecycle_copy_assign<Type>(),
                        .move_assign    = lifecycle_move_assign<Type>(),
                    },
                .lifecycle_context            = nullptr,
                .composite_kind_tag           = CompositeKind::None,
                .trivially_destructible       = std::is_trivially_destructible_v<Type>,
                .trivially_copyable           = std::is_trivially_copyable_v<Type>,
                .trivially_move_constructible = std::is_trivially_move_constructible_v<Type>,
            };
            return plan;
        }

        /** ``StorageLayout`` derived from ``sizeof`` / ``alignof`` of ``T``. */
        template <typename T> [[nodiscard]] static constexpr StorageLayout layout_for() noexcept {
            using Type = std::remove_cv_t<std::remove_reference_t<T>>;
            return {sizeof(Type), alignof(Type)};
        }

        /** Pointer arithmetic helper: ``memory + offset`` bytes. */
        [[nodiscard]] static void *advance(void *memory, size_t offset) noexcept {
            return static_cast<void *>(static_cast<std::byte *>(memory) + offset);
        }

        /** Const overload of ``advance``. */
        [[nodiscard]] static const void *advance(const void *memory, size_t offset) noexcept {
            return static_cast<const void *>(static_cast<const std::byte *>(memory) + offset);
        }

        /** ``std::launder``-wrapped reinterpret cast for safely viewing typed memory. */
        template <typename T> [[nodiscard]] static T *cast(void *memory) noexcept {
            return std::launder(reinterpret_cast<T *>(memory));
        }

        /** Const overload of ``cast``. */
        template <typename T> [[nodiscard]] static const T *cast(const void *memory) noexcept {
            return std::launder(reinterpret_cast<const T *>(memory));
        }

      private:
        template <typename T> [[nodiscard]] static constexpr LifecycleOps::construct_fn lifecycle_construct() noexcept {
            if constexpr (std::is_default_constructible_v<T>) {
                return &default_construct<T>;
            } else {
                return nullptr;
            }
        }

        template <typename T> [[nodiscard]] static constexpr LifecycleOps::destroy_fn lifecycle_destroy() noexcept {
            if constexpr (std::is_trivially_destructible_v<T>) {
                return nullptr;
            } else {
                return &destroy<T>;
            }
        }

        template <typename T> [[nodiscard]] static constexpr LifecycleOps::copy_construct_fn lifecycle_copy_construct() noexcept {
            if constexpr (std::is_copy_constructible_v<T>) {
                return &copy_construct<T>;
            } else {
                return nullptr;
            }
        }

        template <typename T> [[nodiscard]] static constexpr LifecycleOps::move_construct_fn lifecycle_move_construct() noexcept {
            if constexpr (std::is_move_constructible_v<T>) {
                return &move_construct<T>;
            } else {
                return nullptr;
            }
        }

        template <typename T> [[nodiscard]] static constexpr LifecycleOps::copy_assign_fn lifecycle_copy_assign() noexcept {
            if constexpr (std::is_copy_assignable_v<T>) {
                return &copy_assign<T>;
            } else {
                return nullptr;
            }
        }

        template <typename T> [[nodiscard]] static constexpr LifecycleOps::move_assign_fn lifecycle_move_assign() noexcept {
            if constexpr (std::is_move_assignable_v<T>) {
                return &move_assign<T>;
            } else {
                return nullptr;
            }
        }

        struct CompositeRegistry
        {
            using PendingComponent = typename CompositePlanBuilder::PendingComponent;

            struct Entry
            {
                StoragePlan                               plan{};
                std::unique_ptr<std::byte[]>              state_block{};
                std::vector<std::unique_ptr<std::string>> name_storage{};
            };

            [[nodiscard]] const StoragePlan &intern(CompositeKind kind, const std::vector<PendingComponent> &components) {
                return m_table.intern(make_signature(kind, components), [&]() { return build_entry(kind, components); }).plan;
            }

            // Composite plans are interned by child-plan POINTER (see make_signature),
            // so this must be cleared whenever the underlying child plans are torn
            // down — otherwise a later plan reusing a freed address gets a stale hit.
            void clear() noexcept { m_table.clear(); }

          private:
            InternTable<std::string, Entry> m_table{};

            [[nodiscard]] static Entry build_entry(CompositeKind kind, const std::vector<PendingComponent> &components) {
                Entry entry;
                entry.state_block = std::make_unique<std::byte[]>(CompositeState::storage_bytes(components.size()));
                auto *state       = std::construct_at(reinterpret_cast<CompositeState *>(entry.state_block.get()),
                                                      CompositeState{.component_count = components.size()});

                CompositeComponent *component_array = state->components();
                StorageLayout       layout{};
                bool                can_default_construct        = true;
                bool                can_copy_construct           = true;
                bool                can_move_construct           = true;
                bool                can_copy_assign              = true;
                bool                can_move_assign              = true;
                bool                trivially_destructible       = true;
                bool                trivially_copyable           = true;
                bool                trivially_move_constructible = true;

                for (size_t index = 0; index < components.size(); ++index) {
                    const PendingComponent &component = components[index];
                    layout.size                       = align_to(layout.size, component.plan->layout.alignment);
                    component_array[index].index      = index;
                    component_array[index].offset     = layout.size;
                    component_array[index].name = component.name.empty() ? nullptr : intern_name(entry, component.name);
                    component_array[index].plan = component.plan;
                    layout.size += component.plan->layout.size;
                    layout.alignment             = std::max(layout.alignment, component.plan->layout.alignment);
                    can_default_construct        = can_default_construct && component.plan->can_default_construct();
                    can_copy_construct           = can_copy_construct && component.plan->can_copy_construct();
                    can_move_construct           = can_move_construct && component.plan->can_move_construct();
                    can_copy_assign              = can_copy_assign && component.plan->can_copy_assign();
                    can_move_assign              = can_move_assign && component.plan->can_move_assign();
                    trivially_destructible       = trivially_destructible && component.plan->trivially_destructible;
                    trivially_copyable           = trivially_copyable && component.plan->trivially_copyable;
                    trivially_move_constructible = trivially_move_constructible && component.plan->trivially_move_constructible;
                }

                layout.size = align_to(layout.size, layout.alignment);

                entry.plan.layout    = layout;
                entry.plan.lifecycle = {
                    .construct      = can_default_construct ? &MemoryUtils::composite_default_construct : nullptr,
                    .destroy        = trivially_destructible ? nullptr : &MemoryUtils::composite_destroy,
                    .copy_construct = can_copy_construct ? &MemoryUtils::composite_copy_construct : nullptr,
                    .move_construct = can_move_construct ? &MemoryUtils::composite_move_construct : nullptr,
                    .copy_assign    = can_copy_assign ? &MemoryUtils::composite_copy_assign : nullptr,
                    .move_assign    = can_move_assign ? &MemoryUtils::composite_move_assign : nullptr,
                };
                entry.plan.lifecycle_context            = state;
                entry.plan.composite_kind_tag           = kind;
                entry.plan.trivially_destructible       = trivially_destructible;
                entry.plan.trivially_copyable           = trivially_copyable;
                entry.plan.trivially_move_constructible = trivially_move_constructible;
                return entry;
            }

            [[nodiscard]] static const char *intern_name(Entry &entry, std::string_view name) {
                for (const auto &stored : entry.name_storage) {
                    if (*stored == name) { return stored->c_str(); }
                }
                auto        stored = std::make_unique<std::string>(name);
                const char *result = stored->c_str();
                entry.name_storage.push_back(std::move(stored));
                return result;
            }

            [[nodiscard]] static std::string make_signature(CompositeKind kind, const std::vector<PendingComponent> &components) {
                std::string signature = kind == CompositeKind::Tuple ? "tuple(" : "named_tuple(";
                for (const PendingComponent &component : components) {
                    if (!component.name.empty()) {
                        signature += component.name;
                        signature.push_back(':');
                    }
                    signature += std::to_string(reinterpret_cast<uintptr_t>(component.plan));
                    signature.push_back(';');
                }
                signature.push_back(')');
                return signature;
            }
        };

        [[nodiscard]] static CompositeRegistry &composite_registry() {
            // StoragePlan pointers are retained by Values and TypeRecords that
            // may be destroyed during static teardown. Keep the registry
            // object alive for the process lifetime so those destructors never
            // dispatch through a plan whose backing Entry has been destroyed.
            static auto *registry = new CompositeRegistry();
            return *registry;
        }

        struct ArrayRegistry
        {
            struct Key
            {
                const StoragePlan *element_plan{nullptr};
                size_t             element_count{0};

                [[nodiscard]] bool operator==(const Key &) const noexcept = default;
            };

            struct KeyHash
            {
                [[nodiscard]] size_t operator()(const Key &key) const noexcept {
                    size_t       seed       = std::hash<const StoragePlan *>{}(key.element_plan);
                    const size_t count_hash = std::hash<size_t>{}(key.element_count);
                    seed ^= count_hash + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
                    return seed;
                }
            };

            struct Entry
            {
                StoragePlan                 plan{};
                std::unique_ptr<ArrayState> state{};
            };

            [[nodiscard]] const StoragePlan &intern(const StoragePlan &element_plan, size_t count) {
                const Key key{
                    .element_plan  = &element_plan,
                    .element_count = count,
                };
                return m_table.intern(key, [&]() { return build_entry(element_plan, count); }).plan;
            }

            // Keyed by element-plan POINTER; clear when the element plans are torn down.
            void clear() noexcept { m_table.clear(); }

          private:
            InternTable<Key, Entry, KeyHash> m_table{};

            [[nodiscard]] static Entry build_entry(const StoragePlan &element_plan, size_t count) {
                const size_t stride = align_to(element_plan.layout.size, element_plan.layout.alignment);
                if (stride != 0 && count > std::numeric_limits<size_t>::max() / stride) {
                    throw std::overflow_error("MemoryUtils::array_plan storage size overflow");
                }

                Entry entry;
                entry.state = std::make_unique<ArrayState>(ArrayState{
                    .element_plan   = &element_plan,
                    .element_count  = count,
                    .element_stride = stride,
                });

                const bool can_default_construct        = count == 0 || element_plan.can_default_construct();
                const bool can_copy_construct           = count == 0 || element_plan.can_copy_construct();
                const bool can_move_construct           = count == 0 || element_plan.can_move_construct();
                const bool can_copy_assign              = count == 0 || element_plan.can_copy_assign();
                const bool can_move_assign              = count == 0 || element_plan.can_move_assign();
                const bool trivially_destructible       = count == 0 || element_plan.trivially_destructible;
                const bool trivially_copyable           = count == 0 || element_plan.trivially_copyable;
                const bool trivially_move_constructible = count == 0 || element_plan.trivially_move_constructible;

                entry.plan.layout = {
                    .size      = stride * count,
                    .alignment = element_plan.layout.alignment,
                };
                entry.plan.lifecycle = {
                    .construct      = can_default_construct ? &MemoryUtils::array_default_construct : nullptr,
                    .destroy        = trivially_destructible ? nullptr : &MemoryUtils::array_destroy,
                    .copy_construct = can_copy_construct ? &MemoryUtils::array_copy_construct : nullptr,
                    .move_construct = can_move_construct ? &MemoryUtils::array_move_construct : nullptr,
                    .copy_assign    = can_copy_assign ? &MemoryUtils::array_copy_assign : nullptr,
                    .move_assign    = can_move_assign ? &MemoryUtils::array_move_assign : nullptr,
                };
                entry.plan.lifecycle_context            = entry.state.get();
                entry.plan.composite_kind_tag           = CompositeKind::Array;
                entry.plan.trivially_destructible       = trivially_destructible;
                entry.plan.trivially_copyable           = trivially_copyable;
                entry.plan.trivially_move_constructible = trivially_move_constructible;
                return entry;
            }
        };

        [[nodiscard]] static ArrayRegistry &array_registry() {
            // Array plans have the same address-published lifetime contract as
            // composite plans; clear_synthesised_plans() still explicitly
            // releases their entries during a test-only registry reset.
            static auto *registry = new ArrayRegistry();
            return *registry;
        }

        struct RawPlanRegistry
        {
            struct Key
            {
                size_t size{0};
                size_t alignment{1};

                [[nodiscard]] bool operator==(const Key &) const noexcept = default;
            };

            struct KeyHash
            {
                [[nodiscard]] size_t operator()(const Key &key) const noexcept {
                    size_t seed = std::hash<size_t>{}(key.size);
                    const size_t alignment_hash = std::hash<size_t>{}(key.alignment);
                    seed ^= alignment_hash + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
                    return seed;
                }
            };

            [[nodiscard]] const StoragePlan &intern(StorageLayout layout) {
                const Key key{.size = layout.size, .alignment = layout.alignment};
                return m_table.intern(key, [&] {
                    return StoragePlan{
                        .layout = layout,
                        .lifecycle = {.construct = &MemoryUtils::raw_default_construct},
                        .lifecycle_context = nullptr,
                        .composite_kind_tag = CompositeKind::None,
                        .trivially_destructible = true,
                        .trivially_copyable = false,
                        .trivially_move_constructible = false,
                    };
                });
            }

            void clear() noexcept { m_table.clear(); }

          private:
            InternTable<Key, StoragePlan, KeyHash> m_table{};
        };

        [[nodiscard]] static RawPlanRegistry &raw_plan_registry() {
            // Raw plans are also retained through erased runtime type records.
            // The registry object must therefore outlive static borrowers.
            static auto *registry = new RawPlanRegistry();
            return *registry;
        }

        static void raw_default_construct(void *, const void *) noexcept {}

        [[nodiscard]] static constexpr size_t align_to(size_t offset, size_t alignment) noexcept {
            if (alignment <= 1) { return offset; }
            const size_t mask = alignment - 1;
            return (offset + mask) & ~mask;
        }

        [[nodiscard]] static void *default_allocate(StorageLayout layout) {
            if (!layout.valid()) { throw std::logic_error("MemoryUtils::AllocatorOps requires a valid layout"); }
            return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
        }

        static void default_deallocate(void *memory, StorageLayout layout) noexcept {
            if (memory != nullptr && layout.valid()) { ::operator delete(memory, std::align_val_t{layout.alignment}); }
        }

        [[nodiscard]] static const CompositeState &checked_composite_state(const void *context) {
            if (context == nullptr) { throw std::logic_error("MemoryUtils composite lifecycle requires context"); }
            return *static_cast<const CompositeState *>(context);
        }

        [[nodiscard]] static const ArrayState &checked_array_state(const void *context) {
            if (context == nullptr) { throw std::logic_error("MemoryUtils array lifecycle requires context"); }
            return *static_cast<const ArrayState *>(context);
        }

        static void composite_default_construct(void *memory, const void *context) {
            const CompositeState &state       = checked_composite_state(context);
            size_t                constructed = 0;
            auto                  rollback    = ::hgraph::make_scope_exit([&]() noexcept {
                for (size_t index = constructed; index > 0; --index) {
                    const CompositeComponent &component = state.components()[index - 1];
                    component.plan->destroy(advance(memory, component.offset));
                }
            });
            for (; constructed < state.component_count; ++constructed) {
                const CompositeComponent &component = state.components()[constructed];
                component.plan->default_construct(advance(memory, component.offset));
            }
            rollback.release();
        }

        static void composite_destroy(void *memory, const void *context) noexcept {
            if (memory == nullptr || context == nullptr) { return; }

            const CompositeState &state = *static_cast<const CompositeState *>(context);
            for (size_t index = state.component_count; index > 0; --index) {
                const CompositeComponent &component = state.components()[index - 1];
                component.plan->destroy(advance(memory, component.offset));
            }
        }

        static void composite_copy_construct(void *dst, const void *src, const void *context) {
            const CompositeState &state       = checked_composite_state(context);
            size_t                constructed = 0;
            auto                  rollback    = ::hgraph::make_scope_exit([&]() noexcept {
                for (size_t index = constructed; index > 0; --index) {
                    const CompositeComponent &component = state.components()[index - 1];
                    component.plan->destroy(advance(dst, component.offset));
                }
            });
            for (; constructed < state.component_count; ++constructed) {
                const CompositeComponent &component = state.components()[constructed];
                component.plan->copy_construct(advance(dst, component.offset), advance(src, component.offset));
            }
            rollback.release();
        }

        static void composite_move_construct(void *dst, void *src, const void *context) {
            const CompositeState &state       = checked_composite_state(context);
            size_t                constructed = 0;
            auto                  rollback    = ::hgraph::make_scope_exit([&]() noexcept {
                for (size_t index = constructed; index > 0; --index) {
                    const CompositeComponent &component = state.components()[index - 1];
                    component.plan->destroy(advance(dst, component.offset));
                }
            });
            for (; constructed < state.component_count; ++constructed) {
                const CompositeComponent &component = state.components()[constructed];
                component.plan->move_construct(advance(dst, component.offset), advance(src, component.offset));
            }
            rollback.release();
        }

        static void composite_copy_assign(void *dst, const void *src, const void *context) {
            const CompositeState &state = checked_composite_state(context);
            for (size_t index = 0; index < state.component_count; ++index) {
                const CompositeComponent &component = state.components()[index];
                component.plan->copy_assign(advance(dst, component.offset), advance(src, component.offset));
            }
        }

        static void composite_move_assign(void *dst, void *src, const void *context) {
            const CompositeState &state = checked_composite_state(context);
            for (size_t index = 0; index < state.component_count; ++index) {
                const CompositeComponent &component = state.components()[index];
                component.plan->move_assign(advance(dst, component.offset), advance(src, component.offset));
            }
        }

        static void array_default_construct(void *memory, const void *context) {
            const ArrayState &state       = checked_array_state(context);
            size_t            constructed = 0;
            auto              rollback    = ::hgraph::make_scope_exit([&]() noexcept {
                for (size_t index = constructed; index > 0; --index) {
                    state.element_plan->destroy(advance(memory, (index - 1) * state.element_stride));
                }
            });
            for (; constructed < state.element_count; ++constructed) {
                state.element_plan->default_construct(advance(memory, constructed * state.element_stride));
            }
            rollback.release();
        }

        static void array_destroy(void *memory, const void *context) noexcept {
            if (memory == nullptr || context == nullptr) { return; }

            const ArrayState &state = *static_cast<const ArrayState *>(context);
            for (size_t index = state.element_count; index > 0; --index) {
                state.element_plan->destroy(advance(memory, (index - 1) * state.element_stride));
            }
        }

        static void array_copy_construct(void *dst, const void *src, const void *context) {
            const ArrayState &state       = checked_array_state(context);
            size_t            constructed = 0;
            auto              rollback    = ::hgraph::make_scope_exit([&]() noexcept {
                for (size_t index = constructed; index > 0; --index) {
                    state.element_plan->destroy(advance(dst, (index - 1) * state.element_stride));
                }
            });
            for (; constructed < state.element_count; ++constructed) {
                const size_t offset = constructed * state.element_stride;
                state.element_plan->copy_construct(advance(dst, offset), advance(src, offset));
            }
            rollback.release();
        }

        static void array_move_construct(void *dst, void *src, const void *context) {
            const ArrayState &state       = checked_array_state(context);
            size_t            constructed = 0;
            auto              rollback    = ::hgraph::make_scope_exit([&]() noexcept {
                for (size_t index = constructed; index > 0; --index) {
                    state.element_plan->destroy(advance(dst, (index - 1) * state.element_stride));
                }
            });
            for (; constructed < state.element_count; ++constructed) {
                const size_t offset = constructed * state.element_stride;
                state.element_plan->move_construct(advance(dst, offset), advance(src, offset));
            }
            rollback.release();
        }

        static void array_copy_assign(void *dst, const void *src, const void *context) {
            const ArrayState &state = checked_array_state(context);
            for (size_t index = 0; index < state.element_count; ++index) {
                const size_t offset = index * state.element_stride;
                state.element_plan->copy_assign(advance(dst, offset), advance(src, offset));
            }
        }

        static void array_move_assign(void *dst, void *src, const void *context) {
            const ArrayState &state = checked_array_state(context);
            for (size_t index = 0; index < state.element_count; ++index) {
                const size_t offset = index * state.element_stride;
                state.element_plan->move_assign(advance(dst, offset), advance(src, offset));
            }
        }

        template <typename T> static void default_construct(void *memory, const void *context) {
            static_cast<void>(context);
            std::construct_at(cast<T>(memory));
        }

        template <typename T> static void destroy(void *memory, const void *context) noexcept {
            static_cast<void>(context);
            std::destroy_at(cast<T>(memory));
        }

        template <typename T> static void copy_construct(void *dst, const void *src, const void *context) {
            static_cast<void>(context);
            std::construct_at(cast<T>(dst), *cast<T>(src));
        }

        template <typename T> static void move_construct(void *dst, void *src, const void *context) {
            static_cast<void>(context);
            std::construct_at(cast<T>(dst), std::move(*cast<T>(src)));
        }

        template <typename T> static void copy_assign(void *dst, const void *src, const void *context) {
            static_cast<void>(context);
            *cast<T>(dst) = *cast<T>(src);
        }

        template <typename T> static void move_assign(void *dst, void *src, const void *context) {
            static_cast<void>(context);
            *cast<T>(dst) = std::move(*cast<T>(src));
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_MEMORY_UTILS_H
