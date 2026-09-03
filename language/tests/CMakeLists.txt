add_test(
    NAME hgraph_language_cli_help
    COMMAND $<TARGET_FILE:hgl> --help
)
set_tests_properties(hgraph_language_cli_help PROPERTIES
    PASS_REGULAR_EXPRESSION "Usage:"
)

add_test(
    NAME hgraph_language_cli_version
    COMMAND $<TARGET_FILE:hgl> --version
)
set_tests_properties(hgraph_language_cli_version PROPERTIES
    PASS_REGULAR_EXPRESSION "hgl ${PROJECT_VERSION} \\(hgraph [0-9]+\\.[0-9]+\\.[0-9]+\\)"
)

# Every guide example parses (developer guide, "Documentation examples").
file(GLOB _hgl_examples CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/../examples/*.hgl)
foreach(_example IN LISTS _hgl_examples)
    get_filename_component(_name ${_example} NAME_WE)
    add_test(
        NAME hgraph_language_example_${_name}
        COMMAND $<TARGET_FILE:hgl> check ${_example}
    )
endforeach()
unset(_hgl_examples)

# Catch2 for the frontend unit tests: reuse the repository's target when the
# language is built as part of hgraph, else find or fetch it.
if(NOT TARGET Catch2::Catch2WithMain)
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
endif()

add_executable(hgl_syntax_tests
    syntax/temporal_tests.cpp
    syntax/lexer_tests.cpp
    syntax/parser_tests.cpp
)
target_compile_features(hgl_syntax_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_syntax_tests PRIVATE hgl::syntax Catch2::Catch2WithMain)
target_compile_definitions(hgl_syntax_tests PRIVATE HGL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../examples")
hgl_apply_warnings(hgl_syntax_tests)

add_test(NAME hgraph_language_syntax COMMAND hgl_syntax_tests)
