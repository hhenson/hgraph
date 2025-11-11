//
// PyNodeScheduler - Python API wrapper for NodeScheduler
//

#ifndef HGRAPH_PY_NODE_SCHEDULER_H
#define HGRAPH_PY_NODE_SCHEDULER_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <nanobind/nanobind.h>

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
        
        void schedule(engine_time_t when);
        void schedule(engine_time_t when, bool force_set);
        
        [[nodiscard]] bool is_scheduled() const;
        [[nodiscard]] engine_time_t last_scheduled_time() const;
        [[nodiscard]] engine_time_t next_scheduled_time() const;
        
        void pop_tag(const std::string& tag);
        void schedule_with_tag(engine_time_t when, const std::string& tag);
        bool has_tag(const std::string& tag) const;
        
        void set_alarm(engine_time_t when, const std::string& tag);
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        static void register_with_nanobind(nb::module_& m);
        
        [[nodiscard]] NodeScheduler* impl() const { return _impl.get(); }
        [[nodiscard]] bool is_valid() const { return _impl.has_value(); }
        
    private:
        ApiPtr<NodeScheduler> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_NODE_SCHEDULER_H

