#pragma once

//
// Helper class for objects that need:
// 1. shared_from_this() - uses std::enable_shared_from_this for its own control block
// 2. Parent navigation - uses a raw pointer (no ownership)
//
// This decouples the control block from the parent relationship:
// - Each object has its own control block (set when shared_ptr is created)
// - Parent pointer is just for navigation, not lifetime management
//

#include <memory>
#include <variant>
#include <type_traits>

namespace hgraph {

    // Forward declaration
    template<typename T, typename... Us>
    class shared_from_this_with_parent;

    /**
     * Specialization for single parent type.
     * 
     * Inherits from std::enable_shared_from_this for proper control block management.
     * Parent is stored as a raw pointer for navigation only.
     * 
     * For embedded value members (where enable_shared_from_this isn't set up),
     * falls back to using the parent's shared_ptr with aliasing constructor.
     */
    template<typename T, typename U>
    class shared_from_this_with_parent<T, U> : public std::enable_shared_from_this<T> {
    public:
        using ptr = std::shared_ptr<T>;
        using parent_ptr = std::shared_ptr<U>;  // For API compatibility
        using parent_raw_ptr = U*;

        /**
         * Get a shared_ptr to this object using its own control block.
         * For single-parent types, no fallback to aliasing constructor is provided
         * to avoid issues with incomplete types.
         */
        [[nodiscard]] ptr shared_from_this() {
            return std::enable_shared_from_this<T>::shared_from_this();
        }

        [[nodiscard]] std::shared_ptr<const T> shared_from_this() const {
            return std::enable_shared_from_this<T>::shared_from_this();
        }

        /**
         * Check if standard shared_from_this() will work (control block is set up).
         */
        [[nodiscard]] bool can_shared_from_this() const noexcept {
            // weak_from_this() returns empty weak_ptr if not managed by shared_ptr
            return !this->weak_from_this().expired();
        }

        /**
         * Get the parent raw pointer (for navigation).
         */
        [[nodiscard]] parent_raw_ptr parent_ptr_raw() const noexcept {
            return _parent_raw;
        }

        /**
         * Get a shared_ptr to parent (if available).
         * This creates a shared_ptr that shares ownership with whatever owns the parent.
         */
        [[nodiscard]] parent_ptr parent() const noexcept {
            // Can't convert raw pointer to shared_ptr safely
            // Return nullptr - callers should use parent_ptr_raw() for navigation
            return nullptr;
        }

        /**
         * Check if parent is set.
         */
        [[nodiscard]] bool has_parent() const noexcept {
            return _parent_raw != nullptr;
        }

        /**
         * Get parent as specific type (raw pointer).
         * Only works if U matches the parent type.
         */
        template<typename V>
        [[nodiscard]] V* parent_as() const noexcept {
            if constexpr (std::is_same_v<V, U>) {
                return _parent_raw;
            } else {
                return nullptr;
            }
        }

        /**
         * Check if parent is of specific type.
         */
        template<typename V>
        [[nodiscard]] bool is_parent_type() const noexcept {
            return std::is_same_v<V, U> && _parent_raw != nullptr;
        }

    protected:
        shared_from_this_with_parent() = default;

        /**
         * Initialize the parent pointer (raw).
         */
        void init_parent(parent_ptr parent) {
            _parent_raw = parent.get();
        }

        void init_parent(parent_raw_ptr parent) {
            _parent_raw = parent;
        }

        /**
         * Set the parent pointer.
         */
        void set_parent(parent_ptr parent) {
            _parent_raw = parent.get();
        }

        void set_parent(parent_raw_ptr parent) {
            _parent_raw = parent;
        }

        // For backward compatibility - these are no-ops now since we use enable_shared_from_this
        void set_self_tracking(ptr) {}
        void clear_self_tracking() {}
        [[nodiscard]] bool is_self_tracking() const noexcept { return can_shared_from_this(); }

        parent_raw_ptr _parent_raw{nullptr};
    };

    /**
     * Specialization for multiple parent types.
     * Uses a variant of raw pointers for navigation.
     * 
     * For embedded value members (where enable_shared_from_this isn't set up),
     * falls back to using the parent's shared_ptr with aliasing constructor.
     */
    template<typename T, typename U1, typename U2, typename... Us>
    class shared_from_this_with_parent<T, U1, U2, Us...> : public std::enable_shared_from_this<T> {
    public:
        using ptr = std::shared_ptr<T>;
        using parent_variant = std::variant<std::shared_ptr<U1>, std::shared_ptr<U2>, std::shared_ptr<Us>...>;
        using parent_raw_variant = std::variant<U1*, U2*, Us*...>;

        /**
         * Get a shared_ptr to this object.
         * First tries the standard enable_shared_from_this mechanism.
         * Falls back to aliasing constructor with parent's shared_ptr for embedded value members.
         */
        [[nodiscard]] ptr shared_from_this() {
            if (can_shared_from_this()) {
                return std::enable_shared_from_this<T>::shared_from_this();
            }
            return _aliased_shared_from_this();
        }

        [[nodiscard]] std::shared_ptr<const T> shared_from_this() const {
            if (can_shared_from_this()) {
                return std::enable_shared_from_this<T>::shared_from_this();
            }
            return _aliased_shared_from_this_const();
        }

        [[nodiscard]] bool can_shared_from_this() const noexcept {
            return !this->weak_from_this().expired();
        }

        /**
         * Get the parent variant (for backward compatibility - returns empty shared_ptrs).
         */
        [[nodiscard]] parent_variant parent() const noexcept {
            return std::visit([](auto* ptr) -> parent_variant {
                using PtrType = std::remove_pointer_t<decltype(ptr)>;
                return std::shared_ptr<PtrType>(nullptr);
            }, _parent_raw_variant);
        }

        /**
         * Get parent as specific type (raw pointer).
         */
        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        [[nodiscard]] U* parent_as() const noexcept {
            if (auto* ptr = std::get_if<U*>(&_parent_raw_variant)) {
                return *ptr;
            }
            return nullptr;
        }

        /**
         * Check if parent is of specific type.
         */
        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        [[nodiscard]] bool is_parent_type() const noexcept {
            return std::holds_alternative<U*>(_parent_raw_variant);
        }

        [[nodiscard]] bool has_parent() const noexcept {
            return std::visit([](auto* ptr) { return ptr != nullptr; }, _parent_raw_variant);
        }

    protected:
        shared_from_this_with_parent() = default;

        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        void init_parent(std::shared_ptr<U> parent) {
            _parent_raw_variant = parent.get();
        }

        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        void init_parent(U* parent) {
            _parent_raw_variant = parent;
        }

        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        void set_parent(std::shared_ptr<U> parent) {
            _parent_raw_variant = parent.get();
        }

        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        void set_parent(U* parent) {
            _parent_raw_variant = parent;
        }

        // For backward compatibility
        void set_self_tracking(ptr) {}
        void clear_self_tracking() {}
        [[nodiscard]] bool is_self_tracking() const noexcept { return can_shared_from_this(); }

        parent_raw_variant _parent_raw_variant;

    private:
        /**
         * Get shared_ptr via aliasing constructor with parent.
         * Used for embedded value members where standard enable_shared_from_this isn't set up.
         * 
         * For TimeSeriesType parents, uses ts_shared_from_this() since TimeSeriesType
         * is a pure interface that doesn't directly inherit from enable_shared_from_this.
         */
        template<typename P>
        [[nodiscard]] auto _get_parent_shared(P* parent_ptr) {
            if constexpr (requires { parent_ptr->shared_from_this(); }) {
                return parent_ptr->shared_from_this();
            } else if constexpr (requires { parent_ptr->ts_shared_from_this(); }) {
                return parent_ptr->ts_shared_from_this();
            } else {
                static_assert(sizeof(P) == 0, "Parent type must have shared_from_this() or ts_shared_from_this()");
            }
        }

        template<typename P>
        [[nodiscard]] auto _get_parent_shared_const(const P* parent_ptr) const {
            if constexpr (requires { parent_ptr->shared_from_this(); }) {
                return parent_ptr->shared_from_this();
            } else if constexpr (requires { parent_ptr->ts_shared_from_this(); }) {
                return parent_ptr->ts_shared_from_this();
            } else {
                static_assert(sizeof(P) == 0, "Parent type must have shared_from_this() or ts_shared_from_this()");
            }
        }

        [[nodiscard]] ptr _aliased_shared_from_this() {
            return std::visit([this](auto* parent_ptr) -> ptr {
                if (parent_ptr == nullptr) {
                    throw std::bad_weak_ptr();  // No parent to alias from
                }
                auto parent_shared = _get_parent_shared(parent_ptr);
                return std::shared_ptr<T>(parent_shared, static_cast<T*>(this));
            }, _parent_raw_variant);
        }

        [[nodiscard]] std::shared_ptr<const T> _aliased_shared_from_this_const() const {
            return std::visit([this](auto* parent_ptr) -> std::shared_ptr<const T> {
                if (parent_ptr == nullptr) {
                    throw std::bad_weak_ptr();  // No parent to alias from
                }
                auto parent_shared = _get_parent_shared_const(parent_ptr);
                return std::shared_ptr<const T>(parent_shared, static_cast<const T*>(this));
            }, _parent_raw_variant);
        }
    };

} // namespace hgraph
