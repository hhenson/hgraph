//
// Wrapper Factory Implementation
//

#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>
#include <hgraph/types/ref.h>

namespace hgraph::api {
    
    /**
     * Generic wrapper caching function.
     * Checks for cached wrapper via intrusive_base::self_py(), creates if needed.
     * 
     * @tparam ImplType The C++ implementation type (must inherit from intrusive_base)
     * @tparam Creator Function/lambda type that creates the wrapper
     * @param impl Pointer to the C++ implementation object
     * @param control_block Control block for lifetime tracking
     * @param creator Function that creates the wrapper: (ImplType*, control_block_ptr) -> Wrapper
     * @return nb::object containing the cached or newly created wrapper
     */
    template<typename ImplType, typename Creator>
    nb::object get_or_create_wrapper(const ImplType* impl, control_block_ptr control_block, Creator&& creator) {
        if (!impl) {
            return nb::none();
        }
        
        // const_cast is safe here - we need non-const to access intrusive_base methods
        auto* mutable_impl = const_cast<ImplType*>(impl);
        
        // Check if we already have a cached Python wrapper
        PyObject* cached_ptr = mutable_impl->self_py();
        if (cached_ptr) {
            return nb::borrow(cached_ptr);
        }
        
        // Create new wrapper using the provided creator
        auto wrapper = creator(mutable_impl, std::move(control_block));
        nb::object py_obj = nb::cast(std::move(wrapper));
        
        // Cache and return
        mutable_impl->set_self_py(py_obj.ptr());
        return py_obj;
    }
    
    nb::object wrap_node(const hgraph::Node* impl, control_block_ptr control_block) {
        return get_or_create_wrapper(impl, std::move(control_block), 
            [](hgraph::Node* impl, control_block_ptr cb) {
                return PyNode(impl, std::move(cb));
            });
    }
    
    nb::object wrap_graph(const hgraph::Graph* impl, control_block_ptr control_block) {
        return get_or_create_wrapper(impl, std::move(control_block),
            [](hgraph::Graph* impl, control_block_ptr cb) {
                return PyGraph(impl, std::move(cb));
            });
    }
    
    nb::object wrap_node_scheduler(const hgraph::NodeScheduler* impl, control_block_ptr control_block) {
        return get_or_create_wrapper(impl, std::move(control_block),
            [](hgraph::NodeScheduler* impl, control_block_ptr cb) {
                return PyNodeScheduler(impl, std::move(cb));
            });
    }
    
    nb::object wrap_input(const hgraph::TimeSeriesInput* impl, control_block_ptr control_block) {
        return get_or_create_wrapper(impl, std::move(control_block),
            [](hgraph::TimeSeriesInput* impl, control_block_ptr cb) {
                // TODO: Implement dynamic type dispatch based on actual C++ type hierarchy
                // For now, just use base wrapper - type-specific methods will be added later
                return PyTimeSeriesInput(impl, std::move(cb));
            });
    }
    
    nb::object wrap_output(const hgraph::TimeSeriesOutput* impl, control_block_ptr control_block) {
        return get_or_create_wrapper(impl, std::move(control_block),
            [](hgraph::TimeSeriesOutput* impl, control_block_ptr cb) {
                // TODO: Implement dynamic type dispatch based on actual C++ type hierarchy
                // For now, just use base wrapper - type-specific methods will be added later
                return PyTimeSeriesOutput(impl, std::move(cb));
            });
    }
    
} // namespace hgraph::api

