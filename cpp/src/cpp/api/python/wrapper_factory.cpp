//
// Wrapper Factory Implementation
//

#include <hgraph/api/python/wrapper_factory.h>
#include <fmt/format.h>
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
#include <hgraph/types/tsl.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/traits.h>
#include <hgraph/api/python/py_evaluation_engine_api.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_traits.h>
#include <hgraph/api/python/py_ts_types.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <stdexcept>

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
        if (!impl) {
            return nb::none();
        }
        
        auto* mutable_impl = const_cast<hgraph::Node*>(impl);
        PyObject* cached_ptr = mutable_impl->self_py();
        if (cached_ptr) {
            return nb::borrow(cached_ptr);
        }
        
        nb::object py_obj;
        if (auto* lvp = dynamic_cast<LastValuePullNode*>(mutable_impl)) {
            py_obj = nb::cast(PyLastValuePullNode(lvp, control_block));
        } else {
            py_obj = nb::cast(PyNode(mutable_impl, control_block));
        }
        
        mutable_impl->set_self_py(py_obj.ptr());
        return py_obj;
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
        if (!impl) {
            return nb::none();
        }
        
        // const_cast is safe here - we need non-const to access intrusive_base methods
        auto* mutable_impl = const_cast<hgraph::TimeSeriesInput*>(impl);
        
        // Check if we already have a cached Python wrapper
        PyObject* cached_ptr = mutable_impl->self_py();
        if (cached_ptr) {
            return nb::borrow(cached_ptr);
        }
        
        // Dynamic type dispatch to create appropriate specialized wrapper
        nb::object py_obj;
        
        // Check for concrete collection types first (non-template)
        // NOTE: Order matters! Check derived types before base types
        if (auto* tsb = dynamic_cast<TimeSeriesBundleInput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesBundleInput(tsb, control_block));
        } else if (auto* tsl = dynamic_cast<TimeSeriesListInput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesListInput(tsl, control_block));
        } else if (auto* tss = dynamic_cast<TimeSeriesSetInput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesSetInput(tss, control_block));
        } 
        // TSD types - these are templates, so check against base TimeSeriesDictInput
        else if (auto* tsd = dynamic_cast<TimeSeriesDictInput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesDictInput(tsd, control_block));
        }
        // TSW types - these are templates, so try all common template types
        else if (dynamic_cast<TimeSeriesWindowInput<bool>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesWindowInput<int64_t>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesWindowInput<double>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesWindowInput<engine_date_t>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesWindowInput<engine_time_t>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesWindowInput<engine_time_delta_t>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesWindowInput<nb::object>*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesWindowInput(mutable_impl, control_block));
        }
        // Reference types
        else if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceInput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesReferenceInput(ts_ref, control_block));
        }
        // Signal types - not a template, just check the concrete type
        else if (auto* signal = dynamic_cast<TimeSeriesSignalInput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesSignalInput(signal, control_block));
        }
        // Value types - try common template types
        else if (dynamic_cast<TimeSeriesValueInput<bool>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesValueInput<int64_t>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesValueInput<double>*>(mutable_impl) ||
                 dynamic_cast<TimeSeriesValueInput<nb::object>*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesValueInput(mutable_impl, control_block));
        }
        // Check for base templated types (these inherit from IndexedTimeSeriesInput)
        else if (dynamic_cast<IndexedTimeSeriesInput*>(mutable_impl)) {
            // Fallback for any other IndexedTimeSeriesInput types
            py_obj = nb::cast(PyTimeSeriesInput(mutable_impl, control_block));
        } else {
            // Final fallback to base wrapper
            py_obj = nb::cast(PyTimeSeriesInput(mutable_impl, control_block));
        }
        
        // Cache and return
        mutable_impl->set_self_py(py_obj.ptr());
        return py_obj;
    }
    
    nb::object wrap_output(const hgraph::TimeSeriesOutput* impl, control_block_ptr control_block) {
        if (!impl) {
            return nb::none();
        }
        
        // const_cast is safe here - we need non-const to access intrusive_base methods
        auto* mutable_impl = const_cast<hgraph::TimeSeriesOutput*>(impl);
        
        // Check if we already have a cached Python wrapper
        PyObject* cached_ptr = mutable_impl->self_py();
        if (cached_ptr) {
            return nb::borrow(cached_ptr);
        }
        
        // Dynamic type dispatch to create appropriate specialized wrapper
        nb::object py_obj;
        
        // Check for concrete collection types first (non-template)
        // NOTE: Order matters! Check derived types before base types
        if (auto* tsv_int = dynamic_cast<TimeSeriesValueOutput<int64_t>*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesValueOutput(tsv_int, control_block));
        } else if (auto* tsv_double = dynamic_cast<TimeSeriesValueOutput<double>*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesValueOutput(tsv_double, control_block));
        } else if (auto* tsv_bool = dynamic_cast<TimeSeriesValueOutput<bool>*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesValueOutput(tsv_bool, control_block));
        } else if (auto* tsv_obj = dynamic_cast<TimeSeriesValueOutput<nb::object>*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesValueOutput(tsv_obj, control_block));
        } else if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceOutput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesReferenceOutput(ts_ref, control_block));
        } else if (auto* tsb = dynamic_cast<TimeSeriesBundleOutput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesBundleOutput(tsb, control_block));
        } else if (auto* tsl = dynamic_cast<TimeSeriesListOutput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesListOutput(tsl, control_block));
        } else if (auto* tss = dynamic_cast<TimeSeriesSetOutput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesSetOutput(tss, control_block));
        } 
        // TSD types - these are templates, so check against base TimeSeriesDictOutput
        else if (auto* tsd = dynamic_cast<TimeSeriesDictOutput*>(mutable_impl)) {
            py_obj = nb::cast(PyTimeSeriesDictOutput(tsd, control_block));
        } else {
            throw std::runtime_error("wrap_output: unsupported TimeSeriesOutput concrete type");
        }
        
        // Cache and return
        mutable_impl->set_self_py(py_obj.ptr());
        return py_obj;
    }
    
    hgraph::Node* unwrap_node(const nb::object& obj) {
        if (auto* py_node = nb::inst_ptr<PyNode>(obj)) {
            return py_node->_impl.get();
        }
        return nullptr;
    }
    
    hgraph::TimeSeriesInput* unwrap_input(const nb::object& obj) {
        if (auto* py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) {
            return py_input->_impl.get();
        }
        return nullptr;
    }
    
    hgraph::TimeSeriesOutput* unwrap_output(const nb::object& obj) {
        if (auto* py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) {
            return py_output->_impl.get();
        }
        return nullptr;
    }
    
    nb::object wrap_evaluation_engine_api(const hgraph::EvaluationEngineApi* impl, control_block_ptr control_block) {
        return get_or_create_wrapper(impl, std::move(control_block),
            [](hgraph::EvaluationEngineApi* impl, control_block_ptr cb) {
                return PyEvaluationEngineApi(impl, std::move(cb));
            });
    }
    
    nb::object wrap_evaluation_clock(const hgraph::EvaluationClock* impl, control_block_ptr control_block) {
        if (impl == nullptr) {
            return nb::none();
        }
        
        return get_or_create_wrapper(impl, control_block,
            [](hgraph::EvaluationClock* mutable_impl, control_block_ptr cb) {
                return PyEvaluationClock(mutable_impl, std::move(cb));
            });
    }
    
    nb::object wrap_traits(const hgraph::Traits* impl, api::control_block_ptr control_block) {
        // Don't cache traits wrappers - traits is a member of Graph, not a separate heap object
        // Caching on intrusive_base could cause issues during graph teardown
        if (!impl) {
            return nb::none();
        }
        auto* mutable_impl = const_cast<hgraph::Traits*>(impl);
        auto wrapper = api::PyTraits(mutable_impl, std::move(control_block));
        return nb::cast(std::move(wrapper));
    }
    
} // namespace hgraph::api

