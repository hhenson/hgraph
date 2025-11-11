//
// API Smart Pointer - Manages lifetime-checked access to implementation objects
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#ifndef HGRAPH_API_PTR_H
#define HGRAPH_API_PTR_H

#include <memory>
#include <stdexcept>
#include <atomic>

namespace hgraph::api {

    /**
     * Control block for tracking graph lifetime.
     * Shared across all API pointers within a graph's lifetime.
     */
    struct ApiControlBlock {
        std::atomic<bool> graph_alive{true};
        
        void mark_dead() {
            graph_alive.store(false, std::memory_order_release);
        }
        
        [[nodiscard]] bool is_alive() const {
            return graph_alive.load(std::memory_order_acquire);
        }
    };
    
    using control_block_ptr = std::shared_ptr<ApiControlBlock>;
    
    /**
     * Smart pointer for API wrappers that validates graph lifetime before dereferencing.
     * - Move-only (not copyable)
     * - Validates graph is alive before accessing implementation
     * - Throws exception if graph has been destroyed
     * 
     * @tparam T The implementation type being wrapped
     */
    template<typename T>
    class ApiPtr {
    public:
        ApiPtr() = default;
        
        // Constructor from raw pointer and control block
        ApiPtr(T* impl, control_block_ptr control_block)
            : _impl(impl)
            , _control_block(std::move(control_block)) {}
        
        // Move constructor
        ApiPtr(ApiPtr&& other) noexcept
            : _impl(other._impl)
            , _control_block(std::move(other._control_block)) {
            other._impl = nullptr;
        }
        
        // Move assignment
        ApiPtr& operator=(ApiPtr&& other) noexcept {
            if (this != &other) {
                _impl = other._impl;
                _control_block = std::move(other._control_block);
                other._impl = nullptr;
            }
            return *this;
        }
        
        // Delete copy constructor and assignment
        ApiPtr(const ApiPtr&) = delete;
        ApiPtr& operator=(const ApiPtr&) = delete;
        
        // Dereference operators - validate before access
        T* operator->() const {
            validate();
            return _impl;
        }
        
        T& operator*() const {
            validate();
            return *_impl;
        }
        
        // Get raw pointer (for internal use - does not validate)
        [[nodiscard]] T* get() const noexcept {
            return _impl;
        }
        
        // Check if pointer is valid (non-null)
        [[nodiscard]] bool has_value() const noexcept {
            return _impl != nullptr;
        }
        
        // Check if graph is still alive
        [[nodiscard]] bool is_graph_alive() const noexcept {
            return _control_block && _control_block->is_alive();
        }
        
        // Explicit bool conversion
        explicit operator bool() const noexcept {
            return has_value();
        }
        
        // Reset the pointer
        void reset() noexcept {
            _impl = nullptr;
            _control_block.reset();
        }
        
    private:
        void validate() const {
            if (!_impl) {
                throw std::runtime_error("ApiPtr: Attempt to dereference null pointer");
            }
            if (!_control_block || !_control_block->is_alive()) {
                throw std::runtime_error("ApiPtr: Attempt to access object after graph destruction");
            }
        }
        
        T* _impl{nullptr};
        control_block_ptr _control_block;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_API_PTR_H

