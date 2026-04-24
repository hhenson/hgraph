#ifndef HGRAPH_CPP_ROOT_MEMORY_UTILS_H
#define HGRAPH_CPP_ROOT_MEMORY_UTILS_H

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
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

#include <hgraph/util/scope.h>
#include <hgraph/util/tagged_ptr.h>

namespace hgraph::v2
{
    struct MemoryUtils
    {
        struct StorageLayout
        {
            size_t size{0};
            size_t alignment{1};

            [[nodiscard]] constexpr bool valid() const noexcept
            {
                return alignment > 0 && std::has_single_bit(alignment);
            }
        };

        enum class CompositeKind : uint8_t
        {
            None,
            Tuple,
            NamedTuple,
        };

        struct StoragePlan;
        struct CompositeComponent;
        struct CompositeState;
        struct CompositePlanBuilder;
        struct AllocatorOps;

        struct LifecycleOps
        {
            using construct_fn = void (*)(void *, const void *);
            using destroy_fn = void (*)(void *, const void *) noexcept;
            using copy_construct_fn = void (*)(void *, const void *, const void *);
            using move_construct_fn = void (*)(void *, void *, const void *);

            construct_fn construct{nullptr};
            destroy_fn destroy{nullptr};
            copy_construct_fn copy_construct{nullptr};
            move_construct_fn move_construct{nullptr};

            [[nodiscard]] constexpr bool can_default_construct() const noexcept { return construct != nullptr; }
            [[nodiscard]] constexpr bool can_destroy() const noexcept { return destroy != nullptr; }
            [[nodiscard]] constexpr bool can_copy_construct() const noexcept { return copy_construct != nullptr; }
            [[nodiscard]] constexpr bool can_move_construct() const noexcept { return move_construct != nullptr; }

            void default_construct_at(void *memory, const void *context = nullptr) const
            {
                if (construct == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a default-construction hook");
                }
                construct(memory, context);
            }

            void destroy_at(void *memory, const void *context = nullptr) const noexcept
            {
                if (destroy != nullptr && memory != nullptr) {
                    destroy(memory, context);
                }
            }

            void copy_construct_at(void *dst, const void *src, const void *context = nullptr) const
            {
                if (copy_construct == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a copy-construction hook");
                }
                copy_construct(dst, src, context);
            }

            void move_construct_at(void *dst, void *src, const void *context = nullptr) const
            {
                if (move_construct == nullptr) {
                    throw std::logic_error("MemoryUtils::LifecycleOps is missing a move-construction hook");
                }
                move_construct(dst, src, context);
            }
        };

        template <size_t InlineBytes = sizeof(void *), size_t InlineAlignment = alignof(void *)>
        struct InlineStoragePolicy
        {
            static_assert(InlineBytes > 0, "Inline storage must reserve at least one byte");
            static_assert(std::has_single_bit(InlineAlignment), "Inline storage alignment must be a power of two");

            static constexpr size_t inline_bytes = InlineBytes;
            static constexpr size_t inline_alignment = InlineAlignment;

            [[nodiscard]] static constexpr size_t storage_alignment() noexcept
            {
                return InlineAlignment > alignof(void *) ? InlineAlignment : alignof(void *);
            }

            [[nodiscard]] static constexpr bool
            can_store_inline(StorageLayout layout, bool trivially_copyable, bool trivially_destructible) noexcept
            {
                return layout.valid() && layout.size <= inline_bytes && layout.alignment <= inline_alignment &&
                       trivially_copyable && trivially_destructible;
            }
        };

        struct CompositeComponent
        {
            size_t index{0};
            size_t offset{0};
            const char *name{nullptr};
            const StoragePlan *plan{nullptr};

            [[nodiscard]] bool is_named() const noexcept { return name != nullptr; }
        };

        struct CompositeState
        {
            size_t component_count{0};

            [[nodiscard]] static constexpr size_t components_offset() noexcept
            {
                constexpr size_t alignment = alignof(CompositeComponent);
                const size_t offset = sizeof(CompositeState);
                if constexpr (alignment <= 1) {
                    return offset;
                } else {
                    const size_t mask = alignment - 1;
                    return (offset + mask) & ~mask;
                }
            }

            [[nodiscard]] static constexpr size_t storage_bytes(size_t component_count) noexcept
            {
                return components_offset() + sizeof(CompositeComponent) * component_count;
            }

            [[nodiscard]] CompositeComponent *components() noexcept
            {
                auto *base = reinterpret_cast<std::byte *>(this) + components_offset();
                return std::launder(reinterpret_cast<CompositeComponent *>(base));
            }

            [[nodiscard]] const CompositeComponent *components() const noexcept
            {
                auto *base = reinterpret_cast<const std::byte *>(this) + components_offset();
                return std::launder(reinterpret_cast<const CompositeComponent *>(base));
            }
        };

        struct StoragePlan
        {
            StorageLayout layout{};
            LifecycleOps lifecycle{};
            const void *lifecycle_context{nullptr};
            CompositeKind composite_kind_tag{CompositeKind::None};
            bool trivially_destructible{false};
            bool trivially_copyable{false};
            bool trivially_move_constructible{false};

            [[nodiscard]] bool valid() const noexcept
            {
                return layout.valid() &&
                       (layout.size > 0 || is_composite() || lifecycle.can_default_construct() ||
                        lifecycle.can_copy_construct() || lifecycle.can_move_construct() || lifecycle.can_destroy());
            }

            [[nodiscard]] constexpr bool can_default_construct() const noexcept { return lifecycle.can_default_construct(); }
            [[nodiscard]] constexpr bool can_copy_construct() const noexcept { return lifecycle.can_copy_construct(); }
            [[nodiscard]] constexpr bool can_move_construct() const noexcept { return lifecycle.can_move_construct(); }
            [[nodiscard]] constexpr bool requires_destroy() const noexcept
            {
                return !trivially_destructible && lifecycle.can_destroy();
            }

            [[nodiscard]] bool is_composite() const noexcept { return composite_kind_tag != CompositeKind::None; }
            [[nodiscard]] bool is_tuple() const noexcept
            {
                return composite_kind_tag == CompositeKind::Tuple;
            }
            [[nodiscard]] bool is_named_tuple() const noexcept
            {
                return composite_kind_tag == CompositeKind::NamedTuple;
            }
            [[nodiscard]] CompositeKind composite_kind() const
            {
                if (!is_composite()) {
                    throw std::logic_error("MemoryUtils::StoragePlan is not composite");
                }
                return composite_kind_tag;
            }
            [[nodiscard]] const CompositeState *composite_state() const noexcept
            {
                return is_composite() ? static_cast<const CompositeState *>(lifecycle_context) : nullptr;
            }
            [[nodiscard]] size_t component_count() const noexcept
            {
                return composite_state() ? composite_state()->component_count : 0;
            }
            [[nodiscard]] const CompositeComponent &component(size_t index) const
            {
                const CompositeState *state = composite_state();
                if (state == nullptr || index >= state->component_count) {
                    throw std::out_of_range("MemoryUtils::StoragePlan component index out of range");
                }
                return state->components()[index];
            }
            [[nodiscard]] const CompositeComponent *find_component(std::string_view name) const noexcept
            {
                if (!is_named_tuple()) {
                    return nullptr;
                }
                const CompositeState *state = composite_state();
                for (size_t index = 0; index < state->component_count; ++index) {
                    const CompositeComponent &component = state->components()[index];
                    if (component.name != nullptr && name == component.name) {
                        return &component;
                    }
                }
                return nullptr;
            }
            [[nodiscard]] const CompositeComponent &component(std::string_view name) const
            {
                if (const CompositeComponent *result = find_component(name); result != nullptr) {
                    return *result;
                }
                throw std::out_of_range("MemoryUtils::StoragePlan field not found");
            }
            [[nodiscard]] std::span<const CompositeComponent> components() const noexcept
            {
                const CompositeState *state = composite_state();
                return state ? std::span<const CompositeComponent>(state->components(), state->component_count)
                             : std::span<const CompositeComponent>{};
            }

            template <typename Policy = InlineStoragePolicy<>>
            [[nodiscard]] constexpr bool stores_inline() const noexcept
            {
                return Policy::can_store_inline(layout, trivially_copyable, trivially_destructible);
            }

            template <typename Policy = InlineStoragePolicy<>>
            [[nodiscard]] constexpr bool requires_deallocate() const noexcept
            {
                return !stores_inline<Policy>();
            }

            void default_construct(void *memory) const
            {
                lifecycle.default_construct_at(memory, lifecycle_context);
            }

            void destroy(void *memory) const noexcept
            {
                lifecycle.destroy_at(memory, lifecycle_context);
            }

            void copy_construct(void *dst, const void *src) const
            {
                lifecycle.copy_construct_at(dst, src, lifecycle_context);
            }

            void move_construct(void *dst, void *src) const
            {
                lifecycle.move_construct_at(dst, src, lifecycle_context);
            }
        };

        struct AllocatorOps
        {
            using allocate_fn = void *(*)(StorageLayout);
            using deallocate_fn = void (*)(void *, StorageLayout) noexcept;

            allocate_fn allocate{&MemoryUtils::default_allocate};
            deallocate_fn deallocate{&MemoryUtils::default_deallocate};

            [[nodiscard]] void *allocate_storage(StorageLayout layout) const
            {
                if (allocate == nullptr) {
                    throw std::logic_error("MemoryUtils::AllocatorOps is missing an allocation hook");
                }
                return allocate(layout);
            }

            void deallocate_storage(void *memory, StorageLayout layout) const noexcept
            {
                if (deallocate != nullptr && memory != nullptr) {
                    deallocate(memory, layout);
                }
            }
        };

        struct CompositePlanBuilder
        {
            struct PendingComponent
            {
                std::string name{};
                const StoragePlan *plan{nullptr};
            };

            explicit CompositePlanBuilder(CompositeKind kind) noexcept
                : m_kind(kind)
            {
            }

            CompositePlanBuilder &reserve(size_t count)
            {
                m_components.reserve(count);
                return *this;
            }

            CompositePlanBuilder &add_plan(const StoragePlan &plan)
            {
                ensure_kind(CompositeKind::Tuple, "add_plan");
                add_pending_component({}, plan);
                return *this;
            }

            template <typename T>
            CompositePlanBuilder &add_type()
            {
                return add_plan(MemoryUtils::plan_for<T>());
            }

            CompositePlanBuilder &add_field(std::string_view name, const StoragePlan &plan)
            {
                ensure_kind(CompositeKind::NamedTuple, "add_field");
                if (name.empty()) {
                    throw std::logic_error("MemoryUtils::CompositePlanBuilder field names must not be empty");
                }
                if (has_field(name)) {
                    throw std::logic_error("MemoryUtils::CompositePlanBuilder field names must be unique");
                }
                add_pending_component(std::string(name), plan);
                return *this;
            }

            template <typename T>
            CompositePlanBuilder &add_field(std::string_view name)
            {
                return add_field(name, MemoryUtils::plan_for<T>());
            }

            [[nodiscard]] const StoragePlan &build() const
            {
                return composite_registry().intern(m_kind, m_components);
            }

          private:
            CompositeKind m_kind{CompositeKind::Tuple};
            std::vector<PendingComponent> m_components{};

            void ensure_kind(CompositeKind expected, std::string_view action) const
            {
                if (m_kind != expected) {
                    throw std::logic_error(
                        std::string("MemoryUtils::CompositePlanBuilder::") + std::string(action) +
                        (expected == CompositeKind::Tuple ? " is only valid for tuple builders"
                                                          : " is only valid for named tuple builders"));
                }
            }

            [[nodiscard]] bool has_field(std::string_view name) const noexcept
            {
                return std::ranges::any_of(m_components, [name](const PendingComponent &component) {
                    return component.name == name;
                });
            }

            void add_pending_component(std::string name, const StoragePlan &plan)
            {
                if (!plan.valid()) {
                    throw std::logic_error("MemoryUtils::CompositePlanBuilder requires valid child plans");
                }
                m_components.push_back(PendingComponent{
                    .name = std::move(name),
                    .plan = &plan,
                });
            }
        };

        template <typename Policy = InlineStoragePolicy<>>
        class StorageHandle
        {
          public:
            StorageHandle() = default;

            explicit StorageHandle(const StoragePlan &plan, const AllocatorOps &allocator = MemoryUtils::allocator())
            {
                construct_owned_default(plan, allocator);
            }

            StorageHandle(const StorageHandle &other)
            {
                if (other.has_value()) {
                    construct_owned_copy(*other.m_plan, other.data(), *other.allocator());
                }
            }

            StorageHandle(StorageHandle &&other) noexcept
            {
                move_from(std::move(other));
            }

            StorageHandle &operator=(const StorageHandle &other)
            {
                if (this != &other) {
                    reset();
                    if (other.has_value()) {
                        construct_owned_copy(*other.m_plan, other.data(), *other.allocator());
                    }
                }
                return *this;
            }

            StorageHandle &operator=(StorageHandle &&other) noexcept
            {
                if (this != &other) {
                    reset();
                    move_from(std::move(other));
                }
                return *this;
            }

            ~StorageHandle()
            {
                reset();
            }

            [[nodiscard]] static StorageHandle owning(const StoragePlan &plan,
                                                      const AllocatorOps &allocator = MemoryUtils::allocator())
            {
                return StorageHandle(plan, allocator);
            }

            [[nodiscard]] static StorageHandle owning_copy(const StoragePlan &plan,
                                                           const void *src,
                                                           const AllocatorOps &allocator = MemoryUtils::allocator())
            {
                StorageHandle handle;
                handle.construct_owned_copy(plan, src, allocator);
                return handle;
            }

            [[nodiscard]] static StorageHandle reference(const StoragePlan &plan,
                                                         void *data,
                                                         const AllocatorOps &allocator = MemoryUtils::allocator()) noexcept
            {
                return StorageHandle(plan, data, allocator);
            }

            [[nodiscard]] bool has_value() const noexcept { return storage_state() != State::Empty; }
            [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
            [[nodiscard]] bool is_owning() const noexcept
            {
                const State state = storage_state();
                return state == State::OwningInline || state == State::OwningHeap;
            }
            [[nodiscard]] bool is_reference() const noexcept { return storage_state() == State::Borrowed; }
            [[nodiscard]] bool stores_inline() const noexcept { return storage_state() == State::OwningInline; }
            [[nodiscard]] bool stores_heap() const noexcept { return storage_state() == State::OwningHeap; }
            [[nodiscard]] const StoragePlan *plan() const noexcept { return m_plan; }
            [[nodiscard]] const AllocatorOps *allocator() const noexcept { return tagged_allocator(); }

            [[nodiscard]] void *data() noexcept
            {
                switch (storage_state()) {
                case State::OwningInline:
                    return static_cast<void *>(m_storage.inline_bytes.data());
                case State::OwningHeap:
                case State::Borrowed:
                    return m_storage.ptr;
                default:
                    return nullptr;
                }
            }

            [[nodiscard]] const void *data() const noexcept
            {
                switch (storage_state()) {
                case State::OwningInline:
                    return static_cast<const void *>(m_storage.inline_bytes.data());
                case State::OwningHeap:
                case State::Borrowed:
                    return m_storage.ptr;
                default:
                    return nullptr;
                }
            }

            template <typename T>
            [[nodiscard]] T *as() noexcept
            {
                return MemoryUtils::cast<T>(data());
            }

            template <typename T>
            [[nodiscard]] const T *as() const noexcept
            {
                return MemoryUtils::cast<T>(data());
            }

            [[nodiscard]] StorageHandle clone() const
            {
                return has_value() ? owning_copy(*m_plan, data(), *allocator()) : StorageHandle{};
            }

            void reset() noexcept
            {
                const State state = storage_state();
                if (state == State::OwningInline || state == State::OwningHeap) {
                    m_plan->destroy(data());
                    if (state == State::OwningHeap) {
                        allocator()->deallocate_storage(m_storage.ptr, m_plan->layout);
                        m_storage.ptr = nullptr;
                    }
                } else if (state == State::Borrowed) {
                    m_storage.ptr = nullptr;
                }

                m_plan = nullptr;
                m_allocator_state.clear();
            }

          private:
            enum class State : uint8_t
            {
                Empty,
                OwningInline,
                OwningHeap,
                Borrowed,
            };

            union Storage
            {
                std::array<std::byte, Policy::inline_bytes> inline_bytes;
                void *ptr;

                constexpr Storage() noexcept
                    : ptr(nullptr)
                {
                }
            };

            using allocator_state_ptr = ::hgraph::tagged_ptr<const AllocatorOps, 2>;

            const StoragePlan *m_plan{nullptr};
            allocator_state_ptr m_allocator_state{};
            Storage m_storage{};

            StorageHandle(const StoragePlan &plan, void *data, const AllocatorOps &allocator) noexcept
                : m_plan(&plan)
                , m_allocator_state(&allocator, static_cast<allocator_state_ptr::storage_type>(State::Borrowed))
            {
                m_storage.ptr = data;
            }

            [[nodiscard]] const AllocatorOps *tagged_allocator() const noexcept
            {
                return m_allocator_state.ptr();
            }

            [[nodiscard]] State storage_state() const noexcept
            {
                return static_cast<State>(m_allocator_state.tag());
            }

            void set_allocator_state(const AllocatorOps *allocator, State state) noexcept
            {
                m_allocator_state.set(allocator, static_cast<allocator_state_ptr::storage_type>(state));
            }

            [[nodiscard]] static State owning_state_for(const StoragePlan &plan) noexcept
            {
                return plan.template stores_inline<Policy>() ? State::OwningInline : State::OwningHeap;
            }

            void abandon_failed_construction() noexcept
            {
                if (storage_state() == State::OwningHeap) {
                    tagged_allocator()->deallocate_storage(m_storage.ptr, m_plan->layout);
                    m_storage.ptr = nullptr;
                }
                m_plan = nullptr;
                m_allocator_state.clear();
            }

            void construct_owned_default(const StoragePlan &plan, const AllocatorOps &allocator)
            {
                if (!plan.valid()) {
                    throw std::logic_error("MemoryUtils::StorageHandle requires a valid plan");
                }

                m_plan = &plan;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(m_plan->layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    m_plan->default_construct(data());
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                m_plan->default_construct(data());
                rollback.release();
            }

            void construct_owned_copy(const StoragePlan &plan, const void *src, const AllocatorOps &allocator)
            {
                if (!plan.valid()) {
                    throw std::logic_error("MemoryUtils::StorageHandle requires a valid plan");
                }

                m_plan = &plan;
                set_allocator_state(&allocator, owning_state_for(plan));

                if (storage_state() == State::OwningHeap) {
                    m_storage.ptr = tagged_allocator()->allocate_storage(m_plan->layout);
                    auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                    m_plan->copy_construct(data(), src);
                    rollback.release();
                    return;
                }

                auto rollback = ::hgraph::make_scope_exit([this]() noexcept { abandon_failed_construction(); });
                m_plan->copy_construct(data(), src);
                rollback.release();
            }

            void move_from(StorageHandle &&other) noexcept
            {
                m_plan = std::exchange(other.m_plan, nullptr);
                m_allocator_state = std::exchange(other.m_allocator_state, allocator_state_ptr{});

                switch (storage_state()) {
                case State::OwningInline:
                    std::memcpy(m_storage.inline_bytes.data(),
                                other.m_storage.inline_bytes.data(),
                                Policy::inline_bytes);
                    break;
                case State::OwningHeap:
                case State::Borrowed:
                    m_storage.ptr = std::exchange(other.m_storage.ptr, nullptr);
                    break;
                default:
                    m_storage.ptr = nullptr;
                    break;
                }
            }
        };

        [[nodiscard]] static const AllocatorOps &allocator() noexcept
        {
            static const AllocatorOps allocator_ops{};
            return allocator_ops;
        }

        [[nodiscard]] static CompositePlanBuilder tuple()
        {
            return CompositePlanBuilder{CompositeKind::Tuple};
        }

        [[nodiscard]] static CompositePlanBuilder named_tuple()
        {
            return CompositePlanBuilder{CompositeKind::NamedTuple};
        }

        [[nodiscard]] static CompositePlanBuilder composite()
        {
            return tuple();
        }

        [[nodiscard]] static const StoragePlan &tuple_plan(std::initializer_list<const StoragePlan *> components)
        {
            auto builder = tuple();
            builder.reserve(components.size());
            for (const StoragePlan *plan : components) {
                if (plan == nullptr) {
                    throw std::logic_error("MemoryUtils::tuple_plan requires non-null child plans");
                }
                builder.add_plan(*plan);
            }
            return builder.build();
        }

        [[nodiscard]] static const StoragePlan &
        named_tuple_plan(std::initializer_list<std::pair<std::string_view, const StoragePlan *>> components)
        {
            auto builder = named_tuple();
            builder.reserve(components.size());
            for (const auto &[name, plan] : components) {
                if (plan == nullptr) {
                    throw std::logic_error("MemoryUtils::named_tuple_plan requires non-null child plans");
                }
                builder.add_field(name, *plan);
            }
            return builder.build();
        }

        [[nodiscard]] static const StoragePlan &composite_plan(std::initializer_list<const StoragePlan *> components)
        {
            return tuple_plan(components);
        }

        template <typename T>
        [[nodiscard]] static const StoragePlan &plan_for() noexcept
        {
            using Type = std::remove_cv_t<std::remove_reference_t<T>>;
            static const StoragePlan plan = {
                .layout = layout_for<Type>(),
                .lifecycle =
                    {
                        .construct = std::is_default_constructible_v<Type> ? &default_construct<Type> : nullptr,
                        .destroy = std::is_trivially_destructible_v<Type> ? nullptr : &destroy<Type>,
                        .copy_construct = std::is_copy_constructible_v<Type> ? &copy_construct<Type> : nullptr,
                        .move_construct = std::is_move_constructible_v<Type> ? &move_construct<Type> : nullptr,
                    },
                .lifecycle_context = nullptr,
                .composite_kind_tag = CompositeKind::None,
                .trivially_destructible = std::is_trivially_destructible_v<Type>,
                .trivially_copyable = std::is_trivially_copyable_v<Type>,
                .trivially_move_constructible = std::is_trivially_move_constructible_v<Type>,
            };
            return plan;
        }

        template <typename T>
        [[nodiscard]] static constexpr StorageLayout layout_for() noexcept
        {
            using Type = std::remove_cv_t<std::remove_reference_t<T>>;
            return {sizeof(Type), alignof(Type)};
        }

        [[nodiscard]] static void *advance(void *memory, size_t offset) noexcept
        {
            return static_cast<void *>(static_cast<std::byte *>(memory) + offset);
        }

        [[nodiscard]] static const void *advance(const void *memory, size_t offset) noexcept
        {
            return static_cast<const void *>(static_cast<const std::byte *>(memory) + offset);
        }

        template <typename T>
        [[nodiscard]] static T *cast(void *memory) noexcept
        {
            return std::launder(reinterpret_cast<T *>(memory));
        }

        template <typename T>
        [[nodiscard]] static const T *cast(const void *memory) noexcept
        {
            return std::launder(reinterpret_cast<const T *>(memory));
        }

      private:
        struct CompositeRegistry
        {
            using PendingComponent = typename CompositePlanBuilder::PendingComponent;

            std::mutex mutex;
            std::unordered_map<std::string, const StoragePlan *> cache;
            std::vector<std::unique_ptr<StoragePlan>> plans;
            std::vector<std::unique_ptr<std::byte[]>> state_blocks;
            std::vector<std::unique_ptr<std::string>> names;

            [[nodiscard]] const StoragePlan &intern(CompositeKind kind, const std::vector<PendingComponent> &components)
            {
                const std::string signature = make_signature(kind, components);
                std::lock_guard<std::mutex> lock(mutex);

                if (const auto it = cache.find(signature); it != cache.end()) {
                    return *it->second;
                }

                auto state_block = std::make_unique<std::byte[]>(CompositeState::storage_bytes(components.size()));
                auto *state = std::construct_at(reinterpret_cast<CompositeState *>(state_block.get()),
                                                CompositeState{.component_count = components.size()});
                CompositeComponent *component_array = state->components();
                StorageLayout layout{};
                bool can_default_construct = true;
                bool can_copy_construct = true;
                bool can_move_construct = true;
                bool trivially_destructible = true;
                bool trivially_copyable = true;
                bool trivially_move_constructible = true;

                for (size_t index = 0; index < components.size(); ++index) {
                    const PendingComponent &component = components[index];
                    layout.size = align_to(layout.size, component.plan->layout.alignment);
                    component_array[index].index = index;
                    component_array[index].offset = layout.size;
                    component_array[index].name = component.name.empty() ? nullptr : intern_name(component.name);
                    component_array[index].plan = component.plan;
                    layout.size += component.plan->layout.size;
                    layout.alignment = std::max(layout.alignment, component.plan->layout.alignment);
                    can_default_construct = can_default_construct && component.plan->can_default_construct();
                    can_copy_construct = can_copy_construct && component.plan->can_copy_construct();
                    can_move_construct = can_move_construct && component.plan->can_move_construct();
                    trivially_destructible = trivially_destructible && component.plan->trivially_destructible;
                    trivially_copyable = trivially_copyable && component.plan->trivially_copyable;
                    trivially_move_constructible =
                        trivially_move_constructible && component.plan->trivially_move_constructible;
                }

                layout.size = align_to(layout.size, layout.alignment);

                auto plan = std::make_unique<StoragePlan>();
                plan->layout = layout;
                plan->lifecycle =
                    {
                        .construct = can_default_construct ? &MemoryUtils::composite_default_construct : nullptr,
                        .destroy = trivially_destructible ? nullptr : &MemoryUtils::composite_destroy,
                        .copy_construct = can_copy_construct ? &MemoryUtils::composite_copy_construct : nullptr,
                        .move_construct = can_move_construct ? &MemoryUtils::composite_move_construct : nullptr,
                    };
                plan->lifecycle_context = state;
                plan->composite_kind_tag = kind;
                plan->trivially_destructible = trivially_destructible;
                plan->trivially_copyable = trivially_copyable;
                plan->trivially_move_constructible = trivially_move_constructible;

                const StoragePlan *result = plan.get();
                state_blocks.push_back(std::move(state_block));
                plans.push_back(std::move(plan));
                cache.emplace(signature, result);
                return *result;
            }

          private:
            [[nodiscard]] const char *intern_name(std::string_view name)
            {
                for (const auto &stored : names) {
                    if (*stored == name) {
                        return stored->c_str();
                    }
                }
                auto stored = std::make_unique<std::string>(name);
                const char *result = stored->c_str();
                names.push_back(std::move(stored));
                return result;
            }

            [[nodiscard]] static std::string make_signature(CompositeKind kind,
                                                            const std::vector<PendingComponent> &components)
            {
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

        [[nodiscard]] static CompositeRegistry &composite_registry()
        {
            static CompositeRegistry registry;
            return registry;
        }

        [[nodiscard]] static constexpr size_t align_to(size_t offset, size_t alignment) noexcept
        {
            if (alignment <= 1) {
                return offset;
            }
            const size_t mask = alignment - 1;
            return (offset + mask) & ~mask;
        }

        [[nodiscard]] static void *default_allocate(StorageLayout layout)
        {
            if (!layout.valid()) {
                throw std::logic_error("MemoryUtils::AllocatorOps requires a valid layout");
            }
            return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
        }

        static void default_deallocate(void *memory, StorageLayout layout) noexcept
        {
            if (memory != nullptr && layout.valid()) {
                ::operator delete(memory, std::align_val_t{layout.alignment});
            }
        }

        [[nodiscard]] static const CompositeState &checked_composite_state(const void *context)
        {
            if (context == nullptr) {
                throw std::logic_error("MemoryUtils composite lifecycle requires context");
            }
            return *static_cast<const CompositeState *>(context);
        }

        static void composite_default_construct(void *memory, const void *context)
        {
            const CompositeState &state = checked_composite_state(context);
            size_t constructed = 0;
            auto rollback = ::hgraph::make_scope_exit([&]() noexcept {
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

        static void composite_destroy(void *memory, const void *context) noexcept
        {
            if (memory == nullptr || context == nullptr) {
                return;
            }

            const CompositeState &state = *static_cast<const CompositeState *>(context);
            for (size_t index = state.component_count; index > 0; --index) {
                const CompositeComponent &component = state.components()[index - 1];
                component.plan->destroy(advance(memory, component.offset));
            }
        }

        static void composite_copy_construct(void *dst, const void *src, const void *context)
        {
            const CompositeState &state = checked_composite_state(context);
            size_t constructed = 0;
            auto rollback = ::hgraph::make_scope_exit([&]() noexcept {
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

        static void composite_move_construct(void *dst, void *src, const void *context)
        {
            const CompositeState &state = checked_composite_state(context);
            size_t constructed = 0;
            auto rollback = ::hgraph::make_scope_exit([&]() noexcept {
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

        template <typename T>
        static void default_construct(void *memory, const void *context)
        {
            static_cast<void>(context);
            std::construct_at(cast<T>(memory));
        }

        template <typename T>
        static void destroy(void *memory, const void *context) noexcept
        {
            static_cast<void>(context);
            std::destroy_at(cast<T>(memory));
        }

        template <typename T>
        static void copy_construct(void *dst, const void *src, const void *context)
        {
            static_cast<void>(context);
            std::construct_at(cast<T>(dst), *cast<T>(src));
        }

        template <typename T>
        static void move_construct(void *dst, void *src, const void *context)
        {
            static_cast<void>(context);
            std::construct_at(cast<T>(dst), std::move(*cast<T>(src)));
        }
    };
}

#endif //HGRAPH_CPP_ROOT_MEMORY_UTILS_H
