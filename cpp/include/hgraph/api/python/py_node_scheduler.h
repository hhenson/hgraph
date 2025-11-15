//
// PyNodeScheduler - Python API wrapper for NodeScheduler
//

#ifndef HGRAPH_PY_NODE_SCHEDULER_H
#define HGRAPH_PY_NODE_SCHEDULER_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/traits.h>  // For engine_time_t
#include <nanobind/nanobind.h>
#include <string>

namespace nb = nanobind;

namespace hgraph {
    struct NodeScheduler;
}

namespace hgraph::api {
    
    class PyNodeScheduler {
    public:
        PyNodeScheduler(NodeScheduler* impl, control_block_ptr control_block);
        
        PyNodeScheduler(PyNodeScheduler&&) noexcept = default;
        PyNodeScheduler& operator=(PyNodeScheduler&&) noexcept = default;
        PyNodeScheduler(const PyNodeScheduler&) = delete;
        PyNodeScheduler& operator=(const PyNodeScheduler&) = delete;
        
        // Schedule methods
        void schedule(engine_time_t when, std::optional<std::string> tag = std::nullopt, bool on_wall_clock = false);
        void schedule(engine_time_delta_t when, std::optional<std::string> tag = std::nullopt, bool on_wall_clock = false);
        
        // Status queries
        [[nodiscard]] bool is_scheduled() const;
        [[nodiscard]] bool is_scheduled_now() const;
        [[nodiscard]] engine_time_t next_scheduled_time() const;
        
        // Tag management
        [[nodiscard]] engine_time_t pop_tag(const std::string& tag);
        [[nodiscard]] engine_time_t pop_tag(const std::string& tag, engine_time_t default_time);
        [[nodiscard]] bool has_tag(const std::string& tag) const;
        
        // Un-schedule
        void un_schedule();
        void un_schedule(const std::string& tag);
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
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
        ApiPtr<NodeScheduler> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_NODE_SCHEDULER_H

