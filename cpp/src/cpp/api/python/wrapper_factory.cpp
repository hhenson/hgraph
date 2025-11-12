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
    
    PyNode wrap_node(const hgraph::Node* impl, control_block_ptr control_block) {
        // const_cast is safe here - wrappers need non-const access for operations like scheduling
        return PyNode(const_cast<hgraph::Node*>(impl), std::move(control_block));
    }
    
    PyGraph wrap_graph(const hgraph::Graph* impl, control_block_ptr control_block) {
        return PyGraph(const_cast<hgraph::Graph*>(impl), std::move(control_block));
    }
    
    PyNodeScheduler wrap_node_scheduler(const hgraph::NodeScheduler* impl, control_block_ptr control_block) {
        return PyNodeScheduler(const_cast<hgraph::NodeScheduler*>(impl), std::move(control_block));
    }
    
    PyTimeSeriesInput wrap_input(const hgraph::TimeSeriesInput* impl, control_block_ptr control_block) {
        if (!impl) {
            return PyTimeSeriesInput(nullptr, std::move(control_block));
        }
        
        // const_cast is safe here - wrappers need non-const access for operations like make_active/make_passive
        auto* mutable_impl = const_cast<hgraph::TimeSeriesInput*>(impl);
        
        // Check specialized types in order (most specific first)
        
        // REF types - check all specialized reference types first
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesWindowReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesWindowInput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesSetReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesSetInput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesDictReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesDictInput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesBundleReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesBundleInput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesListReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesListInput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesValueReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesValueInput(ref, std::move(control_block));
        }
        // Generic REF type
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesReferenceInput*>(mutable_impl)) {
            return PyTimeSeriesReferenceInput(ref, std::move(control_block));
        }
        
        // TSW (Window)
        if (auto* tsw = dynamic_cast<hgraph::TimeSeriesWindowInput*>(mutable_impl)) {
            return PyTimeSeriesWindowInput(tsw, std::move(control_block));
        }
        
        // TSS (Set)
        if (auto* tss = dynamic_cast<hgraph::TimeSeriesSetInput*>(mutable_impl)) {
            return PyTimeSeriesSetInput(tss, std::move(control_block));
        }
        
        // TSD (Dict)
        if (auto* tsd = dynamic_cast<hgraph::TimeSeriesDictInput*>(mutable_impl)) {
            return PyTimeSeriesDictInput(tsd, std::move(control_block));
        }
        
        // TSB (Bundle)
        if (auto* tsb = dynamic_cast<hgraph::TimeSeriesBundleInput*>(mutable_impl)) {
            return PyTimeSeriesBundleInput(tsb, std::move(control_block));
        }
        
        // TSL (List)
        if (auto* tsl = dynamic_cast<hgraph::TimeSeriesListInput*>(mutable_impl)) {
            return PyTimeSeriesListInput(tsl, std::move(control_block));
        }
        
        // Signal
        if (auto* sig = dynamic_cast<hgraph::TimeSeriesSignalInput*>(mutable_impl)) {
            return PyTimeSeriesSignalInput(sig, std::move(control_block));
        }
        
        // TS (Value) - check TimeSeriesValueInput
        if (auto* ts = dynamic_cast<hgraph::TimeSeriesValueInput*>(mutable_impl)) {
            return PyTimeSeriesValueInput(ts, std::move(control_block));
        }
        
        // Fallback to base wrapper
        return PyTimeSeriesInput(mutable_impl, std::move(control_block));
    }
    
    PyTimeSeriesOutput wrap_output(const hgraph::TimeSeriesOutput* impl, control_block_ptr control_block) {
        if (!impl) {
            return PyTimeSeriesOutput(nullptr, std::move(control_block));
        }
        
        // const_cast is safe here - wrappers need non-const access for operations like set_value
        auto* mutable_impl = const_cast<hgraph::TimeSeriesOutput*>(impl);
        
        // Check specialized types in order (most specific first)
        
        // REF types - check all specialized reference types first
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesWindowReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesWindowOutput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesSetReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesSetOutput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesDictReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesDictOutput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesBundleReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesBundleOutput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesListReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesListOutput(ref, std::move(control_block));
        }
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesValueReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesValueOutput(ref, std::move(control_block));
        }
        // Generic REF type
        if (auto* ref = dynamic_cast<hgraph::TimeSeriesReferenceOutput*>(mutable_impl)) {
            return PyTimeSeriesReferenceOutput(ref, std::move(control_block));
        }
        
        // TSW (Window)
        if (auto* tsw = dynamic_cast<hgraph::TimeSeriesWindowOutput*>(mutable_impl)) {
            return PyTimeSeriesWindowOutput(tsw, std::move(control_block));
        }
        
        // TSS (Set)
        if (auto* tss = dynamic_cast<hgraph::TimeSeriesSetOutput*>(mutable_impl)) {
            return PyTimeSeriesSetOutput(tss, std::move(control_block));
        }
        
        // TSD (Dict)
        if (auto* tsd = dynamic_cast<hgraph::TimeSeriesDictOutput*>(mutable_impl)) {
            return PyTimeSeriesDictOutput(tsd, std::move(control_block));
        }
        
        // TSB (Bundle)
        if (auto* tsb = dynamic_cast<hgraph::TimeSeriesBundleOutput*>(mutable_impl)) {
            return PyTimeSeriesBundleOutput(tsb, std::move(control_block));
        }
        
        // TSL (List)
        if (auto* tsl = dynamic_cast<hgraph::TimeSeriesListOutput*>(mutable_impl)) {
            return PyTimeSeriesListOutput(tsl, std::move(control_block));
        }
        
        // Signal
        if (auto* sig = dynamic_cast<hgraph::TimeSeriesSignalOutput*>(mutable_impl)) {
            return PyTimeSeriesSignalOutput(sig, std::move(control_block));
        }
        
        // TS (Value) - check TimeSeriesValueOutput
        if (auto* ts = dynamic_cast<hgraph::TimeSeriesValueOutput*>(mutable_impl)) {
            return PyTimeSeriesValueOutput(ts, std::move(control_block));
        }
        
        // Fallback to base wrapper
        return PyTimeSeriesOutput(mutable_impl, std::move(control_block));
    }
    
} // namespace hgraph::api

