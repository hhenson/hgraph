find_package(hgraph CONFIG REQUIRED)

add_executable(hgraph_mock_executor_consumer ../mock_executor.cpp)
target_link_libraries(hgraph_mock_executor_consumer PRIVATE hgraph::wiring)
if(COMMAND hgraph_link_python_embedding)
    hgraph_link_python_embedding(hgraph_mock_executor_consumer)
endif()
