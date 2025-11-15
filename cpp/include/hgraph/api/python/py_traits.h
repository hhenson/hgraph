//
// PyTraits - Python API wrapper for Traits
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#ifndef HGRAPH_PY_TRAITS_H
#define HGRAPH_PY_TRAITS_H

#include <hgraph/api/python/api_ptr.h>
#include <nanobind/nanobind.h>
#include <string>

namespace nb = nanobind;

namespace hgraph {
    // Forward declarations for impl types
    struct Traits;
}

namespace hgraph::api {
    
    /**
     * PyTraits - Python API wrapper for Traits implementation
     * 
     * Exposes the public API for accessing graph traits.
     * Move-only, delegates to Traits implementation.
     */
    class PyTraits {
    public:
        // Constructor from implementation and control block
        PyTraits(Traits* impl, control_block_ptr control_block);
        
        // Move constructor and assignment
        PyTraits(PyTraits&&) noexcept = default;
        PyTraits& operator=(PyTraits&&) noexcept = default;
        
        // Delete copy constructor and assignment
        PyTraits(const PyTraits&) = delete;
        PyTraits& operator=(const PyTraits&) = delete;
        
        // Methods exposed to Python
        [[nodiscard]] nb::object get_trait(const std::string& trait_name) const;
        void set_traits(nb::kwargs traits);
        [[nodiscard]] nb::object get_trait_or(const std::string& trait_name, nb::object def_value = nb::none()) const;
        [[nodiscard]] nb::object copy() const;
        
        // Nanobind registration (as "Traits" in Python)
        static void register_with_nanobind(nb::module_& m);
        
        /**
         * Check if this wrapper is valid and usable.
         * Returns false if:
         * - The wrapper is empty/moved-from, OR
         * - The graph has been destroyed/disposed
         */
        [[nodiscard]] bool is_valid() const { 
            return _impl.has_value() && _impl.is_graph_alive(); 
        }
        
    private:
        ApiPtr<Traits> _impl;
        
        // Friend declarations for factory functions
        friend nb::object wrap_traits(const hgraph::Traits* impl, control_block_ptr control_block);
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_TRAITS_H

