//
// Created by Claude for arena allocation support
//

#ifndef ARENA_ENABLE_SHARED_FROM_THIS_H
#define ARENA_ENABLE_SHARED_FROM_THIS_H

#include <memory>
#include <stdexcept>
#include <type_traits>

namespace hgraph {

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

/**
 * Helper for heap allocation that properly initializes arena_enable_shared_from_this.
 * Use this instead of std::make_shared for types that use arena_enable_shared_from_this.
 *
 * This function handles the case where ConcreteType inherits from arena_enable_shared_from_this<BaseType>
 * rather than arena_enable_shared_from_this<ConcreteType>.
 */
template <typename T, typename... Args>
std::shared_ptr<T> arena_make_shared(Args &&...args) {
    auto sp = std::make_shared<T>(std::forward<Args>(args)...);
    // Initialize for heap-allocated objects - must check the inheritance chain
    // T may inherit from arena_enable_shared_from_this<SomeBaseType>, not arena_enable_shared_from_this<T>
    if constexpr (std::is_base_of_v<arena_enable_shared_from_this<T>, T>) {
        _arena_init_weak_this(static_cast<arena_enable_shared_from_this<T> *>(sp.get()), sp);
    }
    return sp;
}

/**
 * Helper for heap allocation when ConcreteType inherits from arena_enable_shared_from_this<BaseType>.
 * Use this when you know the actual base type that has arena_enable_shared_from_this.
 *
 * @tparam ConcreteType The actual type to create
 * @tparam BaseType The base type that has arena_enable_shared_from_this<BaseType>
 */
template <typename ConcreteType, typename BaseType, typename... Args>
std::shared_ptr<ConcreteType> arena_make_shared_as(Args &&...args) {
    auto sp = std::make_shared<ConcreteType>(std::forward<Args>(args)...);
    // Initialize arena_enable_shared_from_this with proper base type
    if constexpr (std::is_base_of_v<arena_enable_shared_from_this<BaseType>, ConcreteType>) {
        auto base_sp = std::static_pointer_cast<BaseType>(sp);
        _arena_init_weak_this(static_cast<arena_enable_shared_from_this<BaseType> *>(sp.get()), base_sp);
    } else if constexpr (std::is_base_of_v<arena_enable_shared_from_this<ConcreteType>, ConcreteType>) {
        _arena_init_weak_this(static_cast<arena_enable_shared_from_this<ConcreteType> *>(sp.get()), sp);
    }
    return sp;
}

}  // namespace hgraph

#endif  // ARENA_ENABLE_SHARED_FROM_THIS_H
