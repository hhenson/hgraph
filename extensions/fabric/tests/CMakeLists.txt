find_package(Catch2 3 CONFIG QUIET)
if(NOT Catch2_FOUND)
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.6.0
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(Catch2)
    target_compile_features(Catch2 PRIVATE cxx_std_17)
endif()

add_executable(hgraph_fabric_tests
    registry_test_listener.cpp
    test_public_contracts.cpp
    test_publication.cpp
    test_resolution.cpp
    test_subscription.cpp
)
if(TARGET hgraph::fabric_kafka)
    target_sources(hgraph_fabric_tests PRIVATE test_kafka_notifier.cpp)
    target_link_libraries(hgraph_fabric_tests
        PRIVATE hgraph::fabric_kafka Catch2::Catch2WithMain)
else()
    target_link_libraries(hgraph_fabric_tests
        PRIVATE hgraph::fabric Catch2::Catch2WithMain)
endif()
target_compile_features(hgraph_fabric_tests PRIVATE cxx_std_23)
target_compile_definitions(hgraph_fabric_tests PRIVATE
    HGRAPH_FABRIC_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures")

add_test(NAME hgraph_fabric.tests COMMAND hgraph_fabric_tests)
if(COMMAND hgraph_configure_test_runtime_paths)
    hgraph_configure_test_runtime_paths(hgraph_fabric_tests)
endif()

add_executable(hgraph_fabric_resolution_perf resolution_perf.cpp)
target_link_libraries(hgraph_fabric_resolution_perf PRIVATE hgraph::fabric)
target_compile_features(hgraph_fabric_resolution_perf PRIVATE cxx_std_23)
