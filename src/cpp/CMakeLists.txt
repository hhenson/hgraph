project(_hgraph)

set(HGRAPH_SOURCES
        python/_hgraph_module.cpp
        python/_hgraph_types.cpp
        python/_hgraph_graph.cpp

        runtime/evaluation_engine.cpp

        types/graph.cpp
        types/node.cpp
        types/scalar_value.cpp
        types/schema.cpp
        types/time_series_type.cpp
        types/ts_type.cpp

        util/lifecycle.cpp
        )

add_definitions(-Dhgraph_EXPORTS)

pybind11_add_module(${PROJECT_NAME} SHARED ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads spdlog::spdlog spdlog::spdlog_header_only Python3::Python)
