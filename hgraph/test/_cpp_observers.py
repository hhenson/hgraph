"""
C++ Observer Switching Module

This module provides switching logic to use C++ implementations of observer classes
when the C++ runtime is enabled.
"""

from hgraph._feature_switch import is_feature_enabled

__all__ = ("setup_cpp_observers",)


def setup_cpp_observers():
    """
    Replace Python observer implementations with C++ versions when C++ runtime is enabled.
    This function is called by _use_cpp_runtime.py during initialization.
    """
    if not is_feature_enabled("use_cpp"):
        return
    
    try:
        import hgraph._hgraph as _hgraph
        import hgraph.test._node_profiler as profiler_module
        import hgraph.test._node_printer as printer_module
        
        # Replace EvaluationProfiler with C++ version
        profiler_module.EvaluationProfiler = _hgraph.EvaluationProfiler
        
        # Replace EvaluationTrace with C++ version
        printer_module.EvaluationTrace = _hgraph.EvaluationTrace
        
        # Expose static methods
        _hgraph.EvaluationTrace.set_print_all_values = _hgraph.EvaluationTrace.set_print_all_values
        _hgraph.EvaluationTrace.set_use_logger = _hgraph.EvaluationTrace.set_use_logger
        
        # Replace in public namespaces
        import hgraph.test
        hgraph.test.EvaluationProfiler = _hgraph.EvaluationProfiler
        hgraph.test.EvaluationTrace = _hgraph.EvaluationTrace
        
        # Replace InspectionObserver with C++ version if debug module is available
        try:
            import hgraph.debug._inspector_observer as inspector_module
            inspector_module.InspectionObserver = _hgraph.InspectionObserver
            inspector_module.GraphInfo = _hgraph.GraphInfo
            
            import hgraph.debug
            hgraph.debug.InspectionObserver = _hgraph.InspectionObserver
            hgraph.debug.GraphInfo = _hgraph.GraphInfo
        except ImportError:
            # Debug module may not be imported yet
            pass
        
        print("C++ Observer implementations enabled")
        
    except Exception as e:
        print(f"Warning: Failed to setup C++ observers: {e}")
        # Don't fail if observer switching fails - fall back to Python implementations

