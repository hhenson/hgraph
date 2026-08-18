# Catch2-based suites (the store tests predate the extraction and keep
# their TEST_CASE structure; the analytics main-based convention does not
# fit them).
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

add_executable(hgraph_persistence_tests
    registry_test_listener.cpp
    test_component_frame.cpp
    test_frame_store.cpp
    test_record_replay_frame.cpp
    test_record_replay_partitioned.cpp
    test_replay_const.cpp
    test_table_persisted.cpp
)
target_link_libraries(hgraph_persistence_tests
    PRIVATE
        hgraph::persistence
        Catch2::Catch2WithMain
)
target_compile_features(hgraph_persistence_tests PRIVATE cxx_std_23)

add_test(NAME hgraph_persistence.tests COMMAND hgraph_persistence_tests)
if(COMMAND hgraph_configure_test_runtime_paths)
    hgraph_configure_test_runtime_paths(hgraph_persistence.tests)
endif()
