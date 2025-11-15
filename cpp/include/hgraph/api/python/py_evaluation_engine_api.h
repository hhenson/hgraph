//
// PyEvaluationEngineApi - Python API wrapper for EvaluationEngineApi
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#ifndef HGRAPH_PY_EVALUATION_ENGINE_API_H
#define HGRAPH_PY_EVALUATION_ENGINE_API_H

#include <hgraph/api/python/api_ptr.h>
#include <nanobind/nanobind.h>
#include <string>

namespace nb = nanobind;

namespace hgraph {
    // Forward declarations for impl types
    struct EvaluationEngineApi;
}

namespace hgraph::api {
    
    /**
     * PyEvaluationEngineApi - Python API wrapper for EvaluationEngineApi implementation
     * 
     * Exposes the public API for accessing evaluation engine functionality.
     * Move-only, delegates to EvaluationEngineApi implementation.
     */
    class PyEvaluationEngineApi {
    public:
        // Constructor from implementation and control block
        PyEvaluationEngineApi(EvaluationEngineApi* impl, control_block_ptr control_block);
        
        // Move constructor and assignment
        PyEvaluationEngineApi(PyEvaluationEngineApi&&) noexcept = default;
        PyEvaluationEngineApi& operator=(PyEvaluationEngineApi&&) noexcept = default;
        
        // Delete copy constructor and assignment
        PyEvaluationEngineApi(const PyEvaluationEngineApi&) = delete;
        PyEvaluationEngineApi& operator=(const PyEvaluationEngineApi&) = delete;
        
        // Properties exposed to Python
        [[nodiscard]] nb::object evaluation_mode() const;
        [[nodiscard]] nb::object start_time() const;
        [[nodiscard]] nb::object end_time() const;
        [[nodiscard]] nb::object evaluation_clock() const;
        
        // Methods exposed to Python
        void request_engine_stop();
        [[nodiscard]] bool is_stop_requested() const;
        void add_before_evaluation_notification(nb::callable fn);
        void add_after_evaluation_notification(nb::callable fn);
        void add_life_cycle_observer(nb::object observer);
        void remove_life_cycle_observer(nb::object observer);
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        // Nanobind registration (as "EvaluationEngineApi" in Python)
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
        ApiPtr<EvaluationEngineApi> _impl;
        
        // Friend declarations for factory functions
        friend nb::object wrap_evaluation_engine_api(const hgraph::EvaluationEngineApi* impl, control_block_ptr control_block);
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_EVALUATION_ENGINE_API_H

