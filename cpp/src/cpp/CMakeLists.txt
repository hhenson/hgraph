project(_hgraph)

set(HGRAPH_SOURCES
        api/python/py_evaluation_clock.cpp
        api/python/py_evaluation_engine.cpp
        api/python/py_graph.cpp
        api/python/py_node.cpp
        api/python/py_special_nodes.cpp
        api/python/py_time_series.cpp
        api/python/py_ref.cpp
        api/python/py_signal.cpp
        api/python/py_ts.cpp
        api/python/py_tsb.cpp
        api/python/py_tsd.cpp
        api/python/py_tsl.cpp
        api/python/py_tss.cpp
        api/python/py_tsw.cpp
        api/python/wrapper_factory.cpp
        builders/builder.cpp
        builders/graph_builder.cpp
        builders/input_builder.cpp
        builders/node_builder.cpp
        builders/output_builder.cpp
        builders/nodes/python_node_builder.cpp
        builders/nodes/python_generator_node_builder.cpp
        builders/nodes/tsd_map_node_builder.cpp
        builders/nodes/reduce_node_builder.cpp
        builders/nodes/context_node_builder.cpp
        builders/nodes/base_nested_graph_node_builder.cpp
        builders/nodes/nested_graph_node_builder.cpp
        builders/nodes/component_node_builder.cpp
        builders/nodes/try_except_node_builder.cpp
        builders/nodes/switch_node_builder.cpp
        builders/nodes/tsd_non_associative_reduce_node_builder.cpp
        builders/nodes/mesh_node_builder.cpp
        builders/nodes/last_value_pull_node_builder.cpp
        builders/time_series_types/time_series_signal_input_builder.cpp
        builders/time_series_types/time_series_value_input_builder.cpp
        builders/time_series_types/specialized_ref_builders.cpp
        builders/time_series_types/time_series_list_input_builder.cpp
        builders/time_series_types/time_series_bundle_input_builder.cpp
        builders/time_series_types/time_series_set_input_builder.cpp
        builders/time_series_types/time_series_window_input_builder.cpp
        builders/time_series_types/time_series_dict_input_builder.cpp
        builders/time_series_types/time_series_value_output_builder.cpp
        builders/time_series_types/time_series_list_output_builder.cpp
        builders/time_series_types/time_series_bundle_output_builder.cpp
        builders/time_series_types/time_series_set_output_builder.cpp
        builders/time_series_types/time_series_window_output_builder.cpp
        builders/time_series_types/time_series_dict_output_builder.cpp

        nodes/base_python_node.cpp
        nodes/component_node.cpp
        nodes/last_value_pull_node.cpp
        nodes/mesh_node.cpp
        nodes/context_node.cpp
        nodes/nested_evaluation_engine.cpp
        nodes/nested_graph_node.cpp
        nodes/nested_node.cpp
        nodes/non_associative_reduce_node.cpp
        nodes/python_generator_node.cpp
        nodes/python_node.cpp
        nodes/push_queue_node.cpp
        nodes/reduce_node.cpp
        nodes/switch_node.cpp
        nodes/try_except_node.cpp
        nodes/tsd_map_node.cpp

        python/_hgraph_builder.cpp
        python/_hgraph_module.cpp
        python/_hgraph_nodes.cpp
        python/_hgraph_runtime.cpp
        python/_hgraph_types.cpp
        python/_hgraph_utils.cpp
        python/global_state.cpp
        python/global_keys.cpp

        runtime/evaluation_context.cpp
        runtime/evaluation_engine.cpp
        runtime/graph_executor.cpp
        runtime/record_replay.cpp
        runtime/observers/evaluation_profiler.cpp
        runtime/observers/evaluation_trace.cpp
        runtime/observers/inspection_observer.cpp
        runtime/observers/observers_bindings.cpp

        types/base_time_series.cpp
        types/constants.cpp
        types/error_type.cpp
        types/feature_extension.cpp
        types/graph.cpp
        types/node.cpp
        types/ref.cpp
        types/scalar_types.cpp
        types/schema_type.cpp
        types/time_series_type.cpp
        types/traits.cpp
        types/ts.cpp
        types/ts_signal.cpp
        types/tsd.cpp
        types/tss.cpp
        types/ts_indexed.cpp
        types/tsb.cpp
        types/tsl.cpp
        types/tsw.cpp

        util/lifecycle.cpp
        util/sender_receiver_state.cpp
        util/stack_trace.cpp
        util/string_util.cpp
)

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} STABLE_ABI ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
target_link_libraries(${PROJECT_NAME} PUBLIC fmt::fmt ddv::ddvisitor)

# Propagate HGRAPH_WITH_BACKWARD to the compiler for conditional compilation
if (HGRAPH_WITH_BACKWARD)
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_WITH_BACKWARD=1)
else ()
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_WITH_BACKWARD=0)
endif ()

# Suppress benign fmt warning on GCC/Clang for this target only (avoid touching ALIAS/IMPORTED fmt targets)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${PROJECT_NAME} PRIVATE -Wno-tautological-compare)
endif ()

# Link Backward only if enabled and available
if (HGRAPH_WITH_BACKWARD AND TARGET Backward::Backward)
    target_link_libraries(${PROJECT_NAME} PRIVATE Backward::Backward)
endif ()
#target_compile_definitions(${PROJECT_NAME} PRIVATE BACKWARD_HAS_BACKTRACE=1)

# Windows: link required debug helper libraries for backward-cpp stack traces
if (WIN32)
    if (HGRAPH_WITH_BACKWARD)
        target_link_libraries(${PROJECT_NAME} PRIVATE Dbghelp Imagehlp)
    endif ()
endif ()

# Install directive for scikit-build-core
# Allow customization of Python module name and install subdirectory
set(HGRAPH_PYMOD_NAME "_hgraph" CACHE STRING "Python extension module name")
set(HGRAPH_PYMOD_INSTALL_SUBDIR "hgraph" CACHE STRING "Install subdir under site-packages")
set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "${HGRAPH_PYMOD_NAME}")
# Install into the package directory (so wheel ends up with hgraph/_hgraph.*)
# Cover Windows by also specifying RUNTIME destination for .pyd files
# (Diagnostics removed to keep logs clean)
install(TARGETS ${PROJECT_NAME}
        LIBRARY DESTINATION ${HGRAPH_PYMOD_INSTALL_SUBDIR}
        RUNTIME DESTINATION ${HGRAPH_PYMOD_INSTALL_SUBDIR}
)

option(HGRAPH_GENERATE_STUBS "Generate nanobind .pyi stubs for the Python module" OFF)
if (HGRAPH_GENERATE_STUBS)
    nanobind_add_stub(
            "${PROJECT_NAME}_stub"
            MODULE ${PROJECT_NAME}
            OUTPUT "${PROJECT_NAME}.pyi"
            PYTHON_PATH $<TARGET_FILE_DIR:${PROJECT_NAME}>
            DEPENDS ${PROJECT_NAME}
    )
endif ()

include(CheckCXXSourceCompiles)
include(CMakePushCheckState)

# Function to check for C++23 stacktrace support
# Sets ${OUT_VAR} to TRUE/FALSE
# Sets ${OUT_LIBS} to the required link libraries (if any)
function(check_cxx23_stacktrace OUT_VAR OUT_LIBS)
    cmake_push_check_state()
    
    # Force C++23 for the check
    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    
    set(SOURCE_CODE "
        #include <stacktrace>
        #include <iostream>
        int main() {
            auto trace = std::stacktrace::current();
            std::cout << trace << std::endl;
            return 0;
        }
    ")

    # 1. Try compiling without extra libraries (e.g. MSVC, Clang with libc++)
    check_cxx_source_compiles("${SOURCE_CODE}" CXX23_STACKTRACE_WORKS_NO_LIB)
    
    if(CXX23_STACKTRACE_WORKS_NO_LIB)
        set(${OUT_VAR} TRUE PARENT_SCOPE)
        set(${OUT_LIBS} "" PARENT_SCOPE)
    else()
        # 2. Try compiling with stdc++_libbacktrace (GCC 12/13)
        set(CMAKE_REQUIRED_LIBRARIES "stdc++_libbacktrace")
        check_cxx_source_compiles("${SOURCE_CODE}" CXX23_STACKTRACE_WORKS_WITH_LIBBACKTRACE)
        
        if(CXX23_STACKTRACE_WORKS_WITH_LIBBACKTRACE)
            set(${OUT_VAR} TRUE PARENT_SCOPE)
            set(${OUT_LIBS} "stdc++_libbacktrace" PARENT_SCOPE)
        else()
            # 3. Try compiling with stdc++exp (GCC 14+)
            set(CMAKE_REQUIRED_LIBRARIES "stdc++exp")
            check_cxx_source_compiles("${SOURCE_CODE}" CXX23_STACKTRACE_WORKS_WITH_LIBEXP)
            
            if(CXX23_STACKTRACE_WORKS_WITH_LIBEXP)
                set(${OUT_VAR} TRUE PARENT_SCOPE)
                set(${OUT_LIBS} "stdc++exp" PARENT_SCOPE)
            else()
                set(${OUT_VAR} FALSE PARENT_SCOPE)
                set(${OUT_LIBS} "" PARENT_SCOPE)
            endif()
        endif()
    endif()

    cmake_pop_check_state()
endfunction()

check_cxx23_stacktrace(HAS_STACKTRACE STACKTRACE_LIBS)
if(HAS_STACKTRACE)
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_HAS_CXX23_STACKTRACE)
    target_link_libraries(${PROJECT_NAME} PRIVATE ${STACKTRACE_LIBS})
endif()
