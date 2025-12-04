#pragma once

//
// API Smart Pointer - Manages lifetime of implementation objects via shared_ptr
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#include <memory>

namespace hgraph {

    using control_block_ptr = std::shared_ptr<void>;

    /**
     * Smart pointer for API wrappers using strong reference semantics.
     * - Stores a shared_ptr to the implementation object
     * - Object lifetime is guaranteed while ApiPtr exists
     *
     * @tparam T The implementation type being wrapped
     */
    template<typename T>
    class ApiPtr {
    public:
        ApiPtr() = default;

        // Constructor from shared_ptr (owns the object directly)
        explicit ApiPtr(std::shared_ptr<T> impl)
            : _impl(std::move(impl)) {}

        // Constructor from raw pointer and donor shared_ptr (creates aliasing shared_ptr)
        // This allows wrapping raw pointers that are owned elsewhere (e.g., by a parent shared_ptr)
        // Accepts const T* and removes const internally
        ApiPtr(const T* impl, control_block_ptr donor)
            : _impl{donor, const_cast<T*>(impl)} {}  // aliasing constructor - shares donor's ref count

        // Move constructor and assignment
        ApiPtr(ApiPtr&& other) noexcept = default;
        ApiPtr& operator=(ApiPtr&& other) noexcept = default;

        // Copy constructor and assignment
        ApiPtr(const ApiPtr&) = default;
        ApiPtr& operator=(const ApiPtr&) = default;

        // Converting constructor from ApiPtr<U> where U* is convertible to T*
        template<typename U>
            requires std::convertible_to<U*, T*>
        ApiPtr(ApiPtr<U> other)
            : _impl(std::static_pointer_cast<T>(other.template control_block_typed<U>())) {}

        // Get control block as typed shared_ptr (for conversion constructor)
        template<typename U>
        [[nodiscard]] std::shared_ptr<U> control_block_typed() const noexcept {
            return std::static_pointer_cast<U>(_impl);
        }

        // Dereference operators
        T* operator->() const { return _impl.get(); }
        T& operator*() const { return *_impl; }

        // Perform a static cast on the result
        template<typename U>
        U* static_cast_() const { return static_cast<U*>(_impl.get()); }

        // Perform a dynamic cast on the result
        template<typename U>
        U* dynamic_cast_() const { return dynamic_cast<U*>(_impl.get()); }

        // Get raw pointer
        [[nodiscard]] T* get() const noexcept { return _impl.get(); }

        // Check if pointer is valid (non-null)
        [[nodiscard]] bool has_value() const noexcept { return _impl != nullptr; }

        // Get control block as shared_ptr<void> for use as donor in aliasing constructors
        [[nodiscard]] control_block_ptr control_block() const noexcept {
            return std::static_pointer_cast<void>(_impl);
        }

        // Explicit bool conversion
        explicit operator bool() const noexcept { return has_value(); }

        // Reset the pointer
        void reset() noexcept { _impl.reset(); }

    private:
        std::shared_ptr<T> _impl;
    };

} // namespace hgraph::api



