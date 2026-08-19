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
    test_object_store.cpp
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

# The store's optional-backend cases compile against the SAME feature answers
# the library was built with. They used to arrive through core's exported
# interface; the extension owns these macros now (RFC 0025 checkpoint 5), so
# the test target states its own dependency on them.
if(HGRAPH_PERSISTENCE_WITH_PARQUET)
    target_compile_definitions(hgraph_persistence_tests PRIVATE HGRAPH_PERSISTENCE_WITH_PARQUET=1)
endif()
if(HGRAPH_PERSISTENCE_WITH_S3)
    target_compile_definitions(hgraph_persistence_tests PRIVATE HGRAPH_PERSISTENCE_WITH_S3=1)
endif()

add_test(NAME hgraph_persistence.tests COMMAND hgraph_persistence_tests)
if(COMMAND hgraph_configure_test_runtime_paths)
    hgraph_configure_test_runtime_paths(hgraph_persistence.tests)
endif()
