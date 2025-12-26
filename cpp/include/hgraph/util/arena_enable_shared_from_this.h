//
// Created by Claude for arena allocation support
//

#ifndef ARENA_ENABLE_SHARED_FROM_THIS_H
#define ARENA_ENABLE_SHARED_FROM_THIS_H

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <typeinfo>

#include <fmt/format.h>

namespace hgraph {

// Global debug flag for arena allocation debugging (defined in builder.cpp)
extern bool arena_debug_mode;

// Canary pattern value - a distinctive pattern that's unlikely to occur naturally
constexpr size_t ARENA_CANARY_PATTERN = 0xDEADBEEFCAFEBABEULL;

// Utility helpers shared between arena allocation helpers and builder sizing logic
inline size_t align_size(size_t current_size, size_t alignment) {
    if (alignment == 0) return current_size;
    size_t remainder = current_size % alignment;
    return remainder == 0 ? current_size : current_size + (alignment - remainder);
}

inline size_t add_canary_size(size_t base_size) {
    if (!arena_debug_mode) {
        return base_size;
    }
    return align_size(base_size, alignof(size_t)) + sizeof(size_t);
}

inline void* set_canary(void* ptr, size_t object_size) {
    if (arena_debug_mode && ptr != nullptr) {
        size_t aligned_size = align_size(object_size, alignof(size_t));
        size_t* canary_ptr = reinterpret_cast<size_t*>(static_cast<char*>(ptr) + aligned_size);
        *canary_ptr = ARENA_CANARY_PATTERN;
    }
    return ptr;
}

inline bool check_canary(const void* ptr, size_t object_size) {
    if (!arena_debug_mode || ptr == nullptr) {
        return true;
    }
    size_t aligned_size = align_size(object_size, alignof(size_t));
    const size_t* canary_ptr = reinterpret_cast<const size_t*>(static_cast<const char*>(ptr) + aligned_size);
    return *canary_ptr == ARENA_CANARY_PATTERN;
}

inline void verify_canary(const void* ptr, size_t object_size, const char* object_name = "object") {
    if (!check_canary(ptr, object_size)) {
        throw std::runtime_error(
            fmt::format(
                "Arena allocation buffer overrun detected for {} at address {:p}. Canary value was overwritten, indicating memory corruption.",
                object_name, ptr));
    }
}

struct ArenaAllocationContext {
    std::shared_ptr<void> buffer;
    size_t                offset{0};
    size_t                total_size{0};
};

inline thread_local ArenaAllocationContext* _arena_allocation_ctx = nullptr;

struct ArenaAllocationGuard {
    explicit ArenaAllocationGuard(ArenaAllocationContext& ctx) : _prev(_arena_allocation_ctx) { _arena_allocation_ctx = &ctx; }
    ~ArenaAllocationGuard() { _arena_allocation_ctx = _prev; }

  private:
    ArenaAllocationContext* _prev;
};

inline ArenaAllocationContext* _arena_current_allocation() { return _arena_allocation_ctx; }

/**
 * Custom enable_shared_from_this that supports arena allocation.
 *
 * The standard std::enable_shared_from_this is automatically initialized by
 * std::make_shared, but NOT by the aliasing shared_ptr constructor used for
 * arena allocation. This class provides the same interface but allows manual
 * initialization via _arena_init_weak_this().
 */
template <typename T>
class arena_enable_shared_from_this {
  public:
    std::shared_ptr<T> shared_from_this() {
        auto sp = _weak_this.lock();
        if (!sp) { throw std::bad_weak_ptr(); }
        return sp;
    }

    std::shared_ptr<const T> shared_from_this() const {
        auto sp = _weak_this.lock();
        if (!sp) { throw std::bad_weak_ptr(); }
        return sp;
    }

    std::weak_ptr<T> weak_from_this() noexcept { return _weak_this; }

    std::weak_ptr<const T> weak_from_this() const noexcept { return _weak_this; }

  protected:
    arena_enable_shared_from_this() noexcept = default;
    arena_enable_shared_from_this(const arena_enable_shared_from_this &) noexcept {}
    arena_enable_shared_from_this &operator=(const arena_enable_shared_from_this &) noexcept { return *this; }
    ~arena_enable_shared_from_this() = default;

    // Friend function for initialization
    template <typename U>
    friend void _arena_init_weak_this(arena_enable_shared_from_this<U> *obj, std::shared_ptr<U> sp);

    mutable std::weak_ptr<T> _weak_this;
};

/**
 * Initialize the weak_this pointer for arena-allocated objects.
 * Called by make_instance_impl after arena allocation.
 */
template <typename T>
void _arena_init_weak_this(arena_enable_shared_from_this<T> *obj, std::shared_ptr<T> sp) {
    if (obj) { obj->_weak_this = sp; }
}

template <typename ConcreteType, typename BaseType>
inline void _arena_init_shared_helpers(ConcreteType* obj_ptr, const std::shared_ptr<ConcreteType>& sp) {
    if constexpr (std::is_base_of_v<arena_enable_shared_from_this<BaseType>, ConcreteType>) {
        auto base_sp = std::static_pointer_cast<BaseType>(sp);
        _arena_init_weak_this(static_cast<arena_enable_shared_from_this<BaseType> *>(obj_ptr), base_sp);
    } else if constexpr (std::is_base_of_v<arena_enable_shared_from_this<ConcreteType>, ConcreteType>) {
        _arena_init_weak_this(static_cast<arena_enable_shared_from_this<ConcreteType> *>(obj_ptr), sp);
    }
}

template <typename ConcreteType, typename BaseType, typename... Args>
std::shared_ptr<ConcreteType> _arena_construct_shared(Args&&... args) {
    if (auto ctx = _arena_current_allocation()) {
        // Align offset for this type
        size_t start_offset = align_size(ctx->offset, alignof(ConcreteType));
        size_t required = start_offset + add_canary_size(sizeof(ConcreteType));
        if (ctx->total_size && required > ctx->total_size) {
            throw std::runtime_error("Arena buffer overflow while constructing object");
        }

        char* buf = static_cast<char*>(ctx->buffer.get()) + start_offset;

        // CRITICAL: Update offset BEFORE placement new to support nested arena allocations.
        // If the constructor calls arena_make_shared_as (e.g., TSD creating its key_set),
        // the nested allocation must see the updated offset to avoid overlapping memory.
        ctx->offset = required;

        if (arena_debug_mode) {
            size_t aligned_obj_size = align_size(sizeof(ConcreteType), alignof(size_t));
            size_t* canary_ptr = reinterpret_cast<size_t*>(buf + aligned_obj_size);
            *canary_ptr = ARENA_CANARY_PATTERN;
        }

        auto* obj_ptr_raw = new (buf) ConcreteType(std::forward<Args>(args)...);
        verify_canary(obj_ptr_raw, sizeof(ConcreteType), typeid(ConcreteType).name());

        auto sp = std::shared_ptr<ConcreteType>(ctx->buffer, obj_ptr_raw);
        _arena_init_shared_helpers<ConcreteType, BaseType>(obj_ptr_raw, sp);
        return sp;
    }

    auto sp = std::make_shared<ConcreteType>(std::forward<Args>(args)...);
    _arena_init_shared_helpers<ConcreteType, BaseType>(sp.get(), sp);
    return sp;
}

/**
 * Helper for heap or arena allocation that properly initializes arena_enable_shared_from_this.
 * Use this instead of std::make_shared for types that use arena_enable_shared_from_this.
 */
template <typename T, typename... Args>
std::shared_ptr<T> arena_make_shared(Args &&...args) {
    return _arena_construct_shared<T, T>(std::forward<Args>(args)...);
}

/**
 * Helper for heap or arena allocation when ConcreteType inherits from arena_enable_shared_from_this<BaseType>.
 * Use this when you know the actual base type that has arena_enable_shared_from_this.
 */
template <typename ConcreteType, typename BaseType, typename... Args>
std::shared_ptr<ConcreteType> arena_make_shared_as(Args &&...args) {
    return _arena_construct_shared<ConcreteType, BaseType>(std::forward<Args>(args)...);
}

}  // namespace hgraph

#endif  // ARENA_ENABLE_SHARED_FROM_THIS_H
