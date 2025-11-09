"""
Quick test of C++ observer implementations
"""
from datetime import datetime, timedelta

def test_cpp_observers():
    """Test that C++ observers work correctly"""
    import hgraph as hg
    
    # Check if C++ runtime is enabled
    cpp_enabled = False
    try:
        import hgraph._hgraph as _hgraph
        print("✓ C++ runtime loaded")
        cpp_enabled = True
        
        # Check observer classes are available
        assert hasattr(_hgraph, 'EvaluationProfiler'), "EvaluationProfiler not found"
        assert hasattr(_hgraph, 'EvaluationTrace'), "EvaluationTrace not found"
        assert hasattr(_hgraph, 'InspectionObserver'), "InspectionObserver not found"
        print("✓ All observer classes found in C++ module")
        
    except ImportError:
        print("⚠ C++ runtime not available, testing Python implementations")
    
    # Test EvaluationProfiler
    print("\n--- Testing EvaluationProfiler ---")
    try:
        from hgraph.test import EvaluationProfiler
        
        # Check if it's using C++ implementation
        if cpp_enabled:
            import hgraph._hgraph as _hgraph
            is_cpp = EvaluationProfiler == _hgraph.EvaluationProfiler
            print(f"  Using {'C++' if is_cpp else 'Python'} implementation")
        
        @hg.graph
        def test_graph():
            c = hg.const(1)
            hg.debug_print("value", c)
        
        profiler = EvaluationProfiler(start=False, eval=False, stop=False, node=False, graph=False)
        result = hg.run_graph(
            test_graph,
            life_cycle_observers=[profiler]
        )
        print("✓ EvaluationProfiler instantiates and runs")
    except Exception as e:
        print(f"✗ EvaluationProfiler failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test EvaluationTrace
    print("\n--- Testing EvaluationTrace ---")
    try:
        from hgraph.test import EvaluationTrace
        
        # Check if it's using C++ implementation
        if cpp_enabled:
            import hgraph._hgraph as _hgraph
            is_cpp = EvaluationTrace == _hgraph.EvaluationTrace
            print(f"  Using {'C++' if is_cpp else 'Python'} implementation")
        
        @hg.graph
        def test_graph2():
            c = hg.const(42)
            hg.debug_print("answer", c)
        
        tracer = EvaluationTrace(start=False, eval=False, stop=False, node=False, graph=False)
        result = hg.run_graph(
            test_graph2,
            life_cycle_observers=[tracer]
        )
        print("✓ EvaluationTrace instantiates and runs")
    except Exception as e:
        print(f"✗ EvaluationTrace failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test InspectionObserver
    print("\n--- Testing InspectionObserver ---")
    try:
        from hgraph.debug import InspectionObserver
        
        # Check if it's using C++ implementation
        if cpp_enabled:
            import hgraph._hgraph as _hgraph
            is_cpp = InspectionObserver == _hgraph.InspectionObserver
            print(f"  Using {'C++' if is_cpp else 'Python'} implementation")
        
        @hg.graph
        def test_graph3():
            c = hg.const(99)
            hg.debug_print("test", c)
        
        inspector = InspectionObserver(compute_sizes=False, track_recent_performance=False)
        result = hg.run_graph(
            test_graph3,
            life_cycle_observers=[inspector]
        )
        print("✓ InspectionObserver instantiates and runs")
        
        # Check that it collected some data
        root_info = inspector.get_graph_info(tuple())
        if root_info:
            print(f"  - Tracked graph with {root_info.node_count} nodes")
            print(f"  - Evaluation count: {root_info.eval_count}")
        
    except Exception as e:
        print(f"✗ InspectionObserver failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n=== Test Complete ===")

if __name__ == '__main__':
    test_cpp_observers()

