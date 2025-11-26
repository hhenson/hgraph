#pragma once

//
// API Smart Pointer - Manages lifetime-checked access to implementation objects
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#include <memory>

namespace hgraph {

    using control_block_ptr = std::shared_ptr<void>;

    /**
     * Smart pointer for API wrappers that uses shared_ptr with control block as donor.
     * - Move-only (not copyable)
     * - Uses aliasing constructor to share control block from arena
     * - Lifetime is managed by shared_ptr, no manual validation needed
     *
     * @tparam T The implementation type being wrapped
     */
    template<typename T>
    class ApiPtr {
    public:
        ApiPtr() = default;

        // Constructor from raw pointer and control block (donor)
        // Uses aliasing constructor: shared_ptr<T>(control_block, raw_ptr)
        ApiPtr(T* impl, control_block_ptr control_block)
            : _impl(control_block, impl) {}

        // Constructor from existing shared_ptr
        // If the shared_ptr already exists, we can use it directly
        explicit ApiPtr(std::shared_ptr<T> impl)
            : _impl(std::move(impl)) {}

        // Move constructor
        ApiPtr(ApiPtr&& other) noexcept
            : _impl(std::move(other._impl)) {}

        // Move assignment
        ApiPtr& operator=(ApiPtr&& other) noexcept {
            if (this != &other) {
                _impl = std::move(other._impl);
            }
            return *this;
        }

        // Delete copy constructor and assignment
        ApiPtr(const ApiPtr&) = delete;
        ApiPtr& operator=(const ApiPtr&) = delete;

        // Dereference operators - shared_ptr handles lifetime
        T* operator->() const noexcept {
            return _impl.get();
        }

        T& operator*() const noexcept {
            return *_impl;
        }

        // Perform a static cast on the result.
        template<typename U>
        std::shared_ptr<U> static_cast_() const noexcept {
            return std::static_pointer_cast<U>(_impl);
        }

        // Perform a dynamic cast on the result.
        template<typename U>
        std::shared_ptr<U> dynamic_cast_() const noexcept {
            return std::dynamic_pointer_cast<U>(_impl);
        }

        // Get raw pointer (for internal use)
        [[nodiscard]] T* get() const noexcept {
            return _impl.get();
        }

        // Check if pointer is valid (non-null)
        [[nodiscard]] bool has_value() const noexcept {
            return _impl != nullptr;
        }

        // Get control block (extracted from shared_ptr's control block)
        [[nodiscard]] control_block_ptr control_block() const noexcept {
            // Extract the control block from the shared_ptr using aliasing constructor
            // Since _impl was constructed with control_block as donor, we can extract it
            // by creating a shared_ptr<void> that aliases from _impl's control block
            return std::shared_ptr<void>(_impl, static_cast<void*>(nullptr));
        }

        // Explicit bool conversion
        explicit operator bool() const noexcept {
            return has_value();
        }

        // Reset the pointer
        void reset() noexcept {
            _impl.reset();
        }

    private:
        std::shared_ptr<T> _impl;
    };



} // namespace hgraph::api



