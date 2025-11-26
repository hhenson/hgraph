#pragma once

//
// Helper class for objects that need shared_from_this but use a parent's control block
// Similar to std::enable_shared_from_this, but uses parent's shared_ptr as donor
// Supports single or multiple parent types (using variant for multiple)
//

#include <memory>
#include <variant>
#include <type_traits>

namespace hgraph {

    // Helper to extract control block from a shared_ptr (for variant visitor)
    // Returns a shared_ptr<void> that shares the control block with the parent
    template<typename U>
    std::shared_ptr<void> extract_control_block(const std::shared_ptr<U>& parent) {
        return std::shared_ptr<void>(parent, static_cast<void*>(nullptr));
    }

    // Forward declaration
    template<typename T, typename... Us>
    class shared_from_this_with_parent;

    /**
     * Specialization for single parent type - stores shared_ptr directly
     */
    template<typename T, typename U>
    class shared_from_this_with_parent<T, U> {
    public:
        using ptr = std::shared_ptr<T>;
        using parent_ptr = std::shared_ptr<U>;

        /**
         * Get a shared_ptr to this object, using the parent's control block.
         * This uses the aliasing constructor: shared_ptr<T>(parent, this)
         */
        [[nodiscard]] ptr shared_from_this() noexcept {
            return ptr(_parent, static_cast<T*>(this));
        }

        /**
         * Get a shared_ptr to this object (const version).
         */
        [[nodiscard]] std::shared_ptr<const T> shared_from_this() const noexcept {
            return std::shared_ptr<const T>(_parent, static_cast<const T*>(this));
        }

        /**
         * Get the parent shared_ptr (const version).
         */
        [[nodiscard]] const parent_ptr& parent() const noexcept {
            return _parent;
        }

        /**
         * Get the parent shared_ptr (non-const version).
         */
        [[nodiscard]] parent_ptr& parent() noexcept {
            return _parent;
        }

    protected:
        /**
         * Protected constructor - derived classes must initialize parent.
         */
        shared_from_this_with_parent() = default;

        /**
         * Initialize the parent pointer. Must be called by derived class constructor.
         */
        void init_parent(parent_ptr parent) {
            _parent = std::move(parent);
        }

        /**
         * Set the parent pointer (for cases where parent is set after construction).
         */
        void set_parent(parent_ptr parent) {
            _parent = std::move(parent);
        }

        /**
         * Protected member - derived classes can access directly if needed.
         */
        parent_ptr _parent;
    };

    /**
     * Specialization for multiple parent types - stores variant
     */
    template<typename T, typename U1, typename U2, typename... Us>
    class shared_from_this_with_parent<T, U1, U2, Us...> {
    public:
        using ptr = std::shared_ptr<T>;
        using parent_variant = std::variant<std::shared_ptr<U1>, std::shared_ptr<U2>, std::shared_ptr<Us>...>;

        /**
         * Get a shared_ptr to this object, using the parent's control block.
         * This uses the aliasing constructor: shared_ptr<T>(parent, this)
         */
        [[nodiscard]] ptr shared_from_this() noexcept {
            // Extract control block from variant
            std::shared_ptr<void> cb = std::visit([](const auto& parent) {
                return extract_control_block(parent);
            }, _parent_variant);
            return ptr(cb, static_cast<T*>(this));
        }

        /**
         * Get a shared_ptr to this object (const version).
         */
        [[nodiscard]] std::shared_ptr<const T> shared_from_this() const noexcept {
            std::shared_ptr<void> cb = std::visit([](const auto& parent) {
                return extract_control_block(parent);
            }, _parent_variant);
            return std::shared_ptr<const T>(cb, static_cast<const T*>(this));
        }

        /**
         * Get the parent variant (const version).
         */
        [[nodiscard]] const parent_variant& parent() const noexcept {
            return _parent_variant;
        }

        /**
         * Get the parent variant (non-const version).
         */
        [[nodiscard]] parent_variant& parent() noexcept {
            return _parent_variant;
        }

        /**
         * Get parent as specific type.
         * Returns nullptr if parent is not of the requested type.
         */
        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        [[nodiscard]] std::shared_ptr<U> parent_as() const noexcept {
            if (auto* ptr = std::get_if<std::shared_ptr<U>>(&_parent_variant)) {
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
            return std::holds_alternative<std::shared_ptr<U>>(_parent_variant);
        }

    protected:
        /**
         * Protected constructor - derived classes must initialize parent.
         */
        shared_from_this_with_parent() = default;

        /**
         * Initialize the parent variant. Must be called by derived class constructor.
         */
        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        void init_parent(std::shared_ptr<U> parent) {
            _parent_variant = std::move(parent);
        }

        /**
         * Set the parent variant (for cases where parent is set after construction).
         */
        template<typename U>
        requires (std::is_same_v<U, U1> || std::is_same_v<U, U2> || (std::is_same_v<U, Us> || ...))
        void set_parent(std::shared_ptr<U> parent) {
            _parent_variant = std::move(parent);
        }

        /**
         * Protected member - derived classes can access directly if needed.
         */
        parent_variant _parent_variant;
    };

} // namespace hgraph

