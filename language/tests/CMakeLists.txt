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

# The resolver over a table of registry names (developer guide, "Frontend
# components": the resolver is hgraph-free).
add_executable(hgl_semantics_tests semantics/resolve_tests.cpp)
target_compile_features(hgl_semantics_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_semantics_tests PRIVATE hgl::semantics Catch2::Catch2WithMain)
target_compile_definitions(hgl_semantics_tests PRIVATE HGL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../examples")
hgl_apply_warnings(hgl_semantics_tests)

add_test(NAME hgraph_language_semantics COMMAND hgl_semantics_tests)

# The direct-wiring backend against the live registry: folding, eval, the
# failure detail, run_program (developer guide, "First pass").
add_executable(hgl_wiring_tests wiring/backend_tests.cpp)
target_compile_features(hgl_wiring_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_wiring_tests PRIVATE hgl::wiring Catch2::Catch2WithMain)
hgl_link_runtime_executable(hgl_wiring_tests)
hgl_apply_warnings(hgl_wiring_tests)

add_test(NAME hgraph_language_wiring COMMAND hgl_wiring_tests)

# The C++ backend, first pass: what `hgl emit-cpp` prints (hgraph-free, over a
# table of kernel names like the resolver tests) ...
add_executable(hgl_codegen_tests codegen/emitter_tests.cpp)
target_compile_features(hgl_codegen_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_codegen_tests PRIVATE hgl::codegen Catch2::Catch2WithMain)
target_compile_definitions(hgl_codegen_tests PRIVATE HGL_CODEGEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/codegen")
hgl_apply_warnings(hgl_codegen_tests)

add_test(NAME hgraph_language_codegen COMMAND hgl_codegen_tests)

# ... and what that C++ does once built: the parity and scalar runtime fixtures
# emitted and compiled through the same `hgl_add_module()` a package uses,
# under the repository's warnings. hgraph's harness evaluates graph parity
# beside `hgl test` and the native-only runtime-node behavior.
hgl_add_module(hgl_codegen_fixture STATIC HGL codegen/parity.hgl codegen/runtime.hgl)
hgl_apply_warnings(hgl_codegen_fixture)

add_executable(hgl_generated_tests codegen/generated_tests.cpp codegen/generated_runtime_tests.cpp)
target_compile_features(hgl_generated_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_generated_tests PRIVATE hgl_codegen_fixture hgl::wiring Catch2::Catch2WithMain)
hgl_link_runtime_executable(hgl_generated_tests)
hgl_apply_warnings(hgl_generated_tests)

add_test(NAME hgraph_language_generated COMMAND hgl_generated_tests)

add_test(
    NAME hgraph_language_test_parity
    COMMAND $<TARGET_FILE:hgl> test ${CMAKE_CURRENT_SOURCE_DIR}/codegen/parity.hgl
)
set_tests_properties(hgraph_language_test_parity PROPERTIES
    PASS_REGULAR_EXPRESSION "6 tests, 0 failed"
)

# The command itself: composition and the first scalar runtime-node slice emit.
add_test(
    NAME hgraph_language_emit_midpoint
    COMMAND $<TARGET_FILE:hgl> emit-cpp ${CMAKE_CURRENT_SOURCE_DIR}/../examples/midpoint.hgl --print
)
set_tests_properties(hgraph_language_emit_midpoint PROPERTIES
    PASS_REGULAR_EXPRESSION "namespace examples::prices"
)
add_test(
    NAME hgraph_language_emit_runtime
    COMMAND $<TARGET_FILE:hgl> emit-cpp ${CMAKE_CURRENT_SOURCE_DIR}/codegen/runtime.hgl --print
)
set_tests_properties(hgraph_language_emit_runtime PROPERTIES
    PASS_REGULAR_EXPRESSION "struct combined_total"
)

# The REPL over a pipe: the plain-line path of the line reader.
add_test(
    NAME hgraph_language_repl_smoke
    COMMAND ${CMAKE_COMMAND}
        -DHGL=$<TARGET_FILE:hgl>
        -DOUT=${CMAKE_CURRENT_BINARY_DIR}/repl
        -P ${CMAKE_CURRENT_SOURCE_DIR}/repl/repl_smoke.cmake
)

# Configure the public package helper as an installed consumer would: there is
# no `hgl` target, the compiler is a file dependency, a keyword module path is
# escaped exactly like emit-cpp, and the Python native target stays beside its
# wrappers even under a multi-config generator.
add_test(
    NAME hgraph_language_cmake_package
    COMMAND ${CMAKE_COMMAND}
        -DHGL=$<TARGET_FILE:hgl>
        -DHELPER=${CMAKE_CURRENT_SOURCE_DIR}/../cmake/HglLanguage.cmake
        -DTEMPLATE=${CMAKE_CURRENT_SOURCE_DIR}/../cmake/hgl_python_module.cpp.in
        -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/cmake/package
        -DOUT=${CMAKE_CURRENT_BINARY_DIR}/cmake-package
        -DPYTHON=${Python_EXECUTABLE}
        "-DGENERATOR=${CMAKE_GENERATOR}"
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/package_test.cmake
)

# The guide's test example passes end to end through the driver
# (user guide, "Testing and running").
add_test(
    NAME hgraph_language_test_midpoint
    COMMAND $<TARGET_FILE:hgl> test ${CMAKE_CURRENT_SOURCE_DIR}/../examples/midpoint.hgl
)
set_tests_properties(hgraph_language_test_midpoint PROPERTIES
    PASS_REGULAR_EXPRESSION "midpoint_ticks \\.\\.\\. ok"
)
