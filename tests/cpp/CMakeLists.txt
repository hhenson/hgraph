project(hg_test)

find_package(Catch2 CONFIG REQUIRED)

enable_testing()


set(HG_UNIT_TEST_SOURCES
        hg/test_hg.cpp
        hg/graph/test_graph.cpp

        hg/graph/typing/test_scalar_value.cpp hg/graph/data_types/basic_type_construction.cpp)

add_executable(${PROJECT_NAME} ${HG_UNIT_TEST_SOURCES} ${HG_INCLUDES})


target_link_libraries(${PROJECT_NAME} PRIVATE _hbase Threads::Threads Catch2::Catch2 Catch2::Catch2WithMain
        pybind11::embed fmt::fmt spdlog::spdlog spdlog::spdlog_header_only)


include(CTest)
include(Catch)

catch_discover_tests(${PROJECT_NAME})