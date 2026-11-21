find_package(hgraph CONFIG REQUIRED)

add_executable(hgraph_scalar_stream_consumer ../scalar_streams.cpp)
target_link_libraries(hgraph_scalar_stream_consumer PRIVATE hgraph::wiring)
if(COMMAND hgraph_link_python_embedding)
    hgraph_link_python_embedding(hgraph_scalar_stream_consumer)
endif()
