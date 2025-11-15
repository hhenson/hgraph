//
// PyEvaluationClock - Python API wrapper for EvaluationClock
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#ifndef HGRAPH_PY_EVALUATION_CLOCK_H
#define HGRAPH_PY_EVALUATION_CLOCK_H

#include <hgraph/api/python/api_ptr.h>
#include <nanobind/nanobind.h>
#include <string>

namespace nb = nanobind;

namespace hgraph {
    // Forward declarations for impl types
    struct EvaluationClock;
}

namespace hgraph::api {
    
    /**
     * PyEvaluationClock - Python API wrapper for EvaluationClock implementation
     * 
     * Exposes the public API for accessing evaluation clock functionality.
     * Move-only, delegates to EvaluationClock implementation.
     */
    class PyEvaluationClock {
    public:
        // Constructor from implementation and control block
        PyEvaluationClock(EvaluationClock* impl, control_block_ptr control_block);
        
        // Move constructor and assignment
        PyEvaluationClock(PyEvaluationClock&&) noexcept = default;
        PyEvaluationClock& operator=(PyEvaluationClock&&) noexcept = default;
        
        // Delete copy constructor and assignment
        PyEvaluationClock(const PyEvaluationClock&) = delete;
        PyEvaluationClock& operator=(const PyEvaluationClock&) = delete;
        
        // Properties exposed to Python
        [[nodiscard]] nb::object evaluation_time() const;
        [[nodiscard]] nb::object now() const;
        [[nodiscard]] nb::object next_cycle_evaluation_time() const;
        [[nodiscard]] nb::object cycle_time() const;
        
        // Nested clock properties (return None if not a nested clock)
        [[nodiscard]] nb::object node() const;
        [[nodiscard]] nb::object key() const;
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        // Nanobind registration (as "EvaluationClock" in Python)
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
        ApiPtr<EvaluationClock> _impl;
        
        // Friend declarations for factory functions
        friend nb::object wrap_evaluation_clock(const hgraph::EvaluationClock* impl, control_block_ptr control_block);
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_EVALUATION_CLOCK_H

