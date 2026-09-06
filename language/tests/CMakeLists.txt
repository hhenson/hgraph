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
    PASS_REGULAR_EXPRESSION "hgl [0-9][^ ]* \\(hgraph api [0-9]+\\.[0-9]+\\.[0-9]+\\)"
)

# Execution backends consume HGraph IR. Keep the final AST/resolver migration
# boundary executable so an accidental frontend dependency fails in CI.
add_test(
    NAME hgraph_language_backend_architecture
    COMMAND ${CMAKE_COMMAND}
        "-DHGL_LANGUAGE_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/../src"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/backend_architecture.cmake"
)
add_test(
    NAME hgraph_language_backend_architecture_rejects_indirect_ast
    COMMAND ${CMAKE_COMMAND}
        "-DHGL_LANGUAGE_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/cmake/backend_architecture_fixture"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/backend_architecture.cmake"
)
set_tests_properties(hgraph_language_backend_architecture_rejects_indirect_ast PROPERTIES
    WILL_FAIL TRUE
    PASS_REGULAR_EXPRESSION "bridge/ast_adapter.h contains forbidden frontend dependency syntax/ast"
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

add_test(
    NAME hgraph_language_dump_hir
    COMMAND $<TARGET_FILE:hgl> check ${CMAKE_CURRENT_SOURCE_DIR}/../examples/stateful-node.hgl --dump-hir
)
set_tests_properties(hgraph_language_dump_hir PROPERTIES
    PASS_REGULAR_EXPRESSION "HIR typed module examples.stateful_node"
)

add_test(
    NAME hgraph_language_dump_hgraph_ir
    COMMAND $<TARGET_FILE:hgl> check ${CMAKE_CURRENT_SOURCE_DIR}/../examples/stateful-node.hgl --dump-hgraph-ir
)
set_tests_properties(hgraph_language_dump_hgraph_ir PROPERTIES
    PASS_REGULAR_EXPRESSION "HGRAPH-IR bodies module examples.stateful_node"
)

add_test(
    NAME hgraph_language_check_descriptor
    COMMAND ${CMAKE_COMMAND}
        -DHGL=$<TARGET_FILE:hgl>
        -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/../examples/midpoint.hgl
        -DOUT=${CMAKE_CURRENT_BINARY_DIR}/descriptor-check
        -P ${CMAKE_CURRENT_SOURCE_DIR}/driver/check_descriptor.cmake
)

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
    syntax/token_grammar_tests.cpp
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

# The first HIR boundary: every resolved name becomes a stable symbol and the
# complete source program is copied into a backend-independent arena.
add_executable(hgl_ir_tests ir/lower_tests.cpp)
target_compile_features(hgl_ir_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_ir_tests PRIVATE hgl::ir Catch2::Catch2WithMain)
target_compile_definitions(hgl_ir_tests PRIVATE HGL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../examples")
hgl_apply_warnings(hgl_ir_tests)

add_test(NAME hgraph_language_ir COMMAND hgl_ir_tests)

# The hgraph-IR boundary owns self-contained canonical contracts and bodies.
add_executable(hgl_graph_ir_tests
    hgraph_ir/complete_tests.cpp
    hgraph_ir/control_flow_tests.cpp
    hgraph_ir/lower_tests.cpp
)
target_compile_features(hgl_graph_ir_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_graph_ir_tests PRIVATE hgl::graph_ir Catch2::Catch2WithMain)
target_compile_definitions(hgl_graph_ir_tests PRIVATE HGL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../examples")
hgl_apply_warnings(hgl_graph_ir_tests)

add_test(NAME hgraph_language_hgraph_ir COMMAND hgl_graph_ir_tests)

# Canonical module descriptors remain a small HGraph-IR backend of their own;
# keep JSON contract tests out of the already expensive code-generation TU.
add_executable(hgl_descriptor_tests descriptor/module_descriptor_tests.cpp)
target_compile_features(hgl_descriptor_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_descriptor_tests PRIVATE hgl::descriptor Catch2::Catch2WithMain)
hgl_apply_warnings(hgl_descriptor_tests)

add_test(NAME hgraph_language_descriptor COMMAND hgl_descriptor_tests)

# Descriptor decoding has a JSON dependency and a deliberately adversarial
# input corpus, so keep it out of the compact writer/snapshot test TU.
add_executable(hgl_descriptor_reader_tests descriptor/module_descriptor_reader_tests.cpp)
target_compile_features(hgl_descriptor_reader_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_descriptor_reader_tests PRIVATE hgl::descriptor_reader Catch2::Catch2WithMain)
hgl_apply_warnings(hgl_descriptor_reader_tests)

add_test(NAME hgraph_language_descriptor_reader COMMAND hgl_descriptor_reader_tests)

add_executable(hgl_native_scalar_descriptor_fixture driver/native_scalar_descriptor_fixture.cpp)
target_compile_features(hgl_native_scalar_descriptor_fixture PRIVATE cxx_std_23)
target_link_libraries(hgl_native_scalar_descriptor_fixture PRIVATE hgl::native_package)
hgl_apply_warnings(hgl_native_scalar_descriptor_fixture)

set(_hgl_native_scalar_descriptor "${CMAKE_CURRENT_BINARY_DIR}/native-scalar-import.hgl-module.json")
add_custom_command(
    OUTPUT "${_hgl_native_scalar_descriptor}"
    COMMAND hgl_native_scalar_descriptor_fixture "${_hgl_native_scalar_descriptor}"
    DEPENDS hgl_native_scalar_descriptor_fixture
    COMMENT "Author native scalar import descriptor"
    VERBATIM
)
add_test(
    NAME hgraph_language_prepare_native_scalar_descriptor
    COMMAND hgl_native_scalar_descriptor_fixture "${_hgl_native_scalar_descriptor}"
)
set_tests_properties(hgraph_language_prepare_native_scalar_descriptor PROPERTIES FIXTURES_SETUP hgl_native_scalar_descriptor)
add_test(
    NAME hgraph_language_check_native_scalar_import
    COMMAND $<TARGET_FILE:hgl> check "${CMAKE_CURRENT_SOURCE_DIR}/driver/native-scalar-import.hgl"
            --module-descriptor "${_hgl_native_scalar_descriptor}" --dump-hgraph-ir
)
set_tests_properties(hgraph_language_check_native_scalar_import PROPERTIES
    FIXTURES_REQUIRED hgl_native_scalar_descriptor
    PASS_REGULAR_EXPRESSION "checks.native_dependency::blend"
)

# Public native-package authoring is a separate, installed-facing API. Keep
# its contract tests distinct from internal descriptor model/reader tests.
add_executable(hgl_native_package_tests native/native_package_tests.cpp)
target_compile_features(hgl_native_package_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_native_package_tests PRIVATE
    hgl::native_package
    hgl::descriptor_reader
    Catch2::Catch2WithMain
)
hgl_apply_warnings(hgl_native_package_tests)

add_test(NAME hgraph_language_native_package COMMAND hgl_native_package_tests)

# Install the public authoring target, compile a consumer without source-tree
# include paths or internal compiler libraries, emit a descriptor, and pass it
# through the installed-facing command contract.
add_test(
    NAME hgraph_language_native_package_install_consumer
    COMMAND ${CMAKE_COMMAND}
        -DBUILD=${CMAKE_BINARY_DIR}
        -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/cmake/native_package
        -DOUT=${CMAKE_CURRENT_BINARY_DIR}/native-package-consumer
        "-DGENERATOR=${CMAKE_GENERATOR}"
        -DHGL=$<TARGET_FILE:hgl>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/native_package_test.cmake
)
set_tests_properties(hgraph_language_native_package_install_consumer PROPERTIES RUN_SERIAL TRUE)

# The direct-wiring backend against the live registry: folding, eval, the
# failure detail, run_program (developer guide, "First pass").
add_executable(hgl_wiring_tests wiring/backend_tests.cpp)
target_compile_features(hgl_wiring_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_wiring_tests PRIVATE hgl::wiring Catch2::Catch2WithMain)
hgl_link_runtime_executable(hgl_wiring_tests)
hgl_apply_warnings(hgl_wiring_tests)

add_test(NAME hgraph_language_wiring COMMAND hgl_wiring_tests)

# Runtime metadata materialization is isolated from the evaluator so type
# coverage remains cheap to compile and failures stay at the IR/runtime seam.
add_executable(hgl_wiring_type_tests wiring/type_bridge_tests.cpp)
target_compile_features(hgl_wiring_type_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_wiring_type_tests PRIVATE hgl::wiring_types Catch2::Catch2WithMain)
hgl_apply_warnings(hgl_wiring_type_tests)

add_test(NAME hgraph_language_wiring_types COMMAND hgl_wiring_type_tests)

# The type checker hands native candidate ranking to the hgraph registry while
# retaining only copied, backend-independent selection data in HIR.
add_executable(hgl_operator_type_tests wiring/operator_types_tests.cpp)
target_compile_features(hgl_operator_type_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_operator_type_tests PRIVATE hgl::wiring Catch2::Catch2WithMain)
hgl_link_runtime_executable(hgl_operator_type_tests)
hgl_apply_warnings(hgl_operator_type_tests)

add_test(NAME hgraph_language_operator_types COMMAND hgl_operator_type_tests)

# The native module boundary translates registration failures through the
# common scope/exception helper rather than maintaining a local catch ladder.
add_executable(hgl_native_module_tests driver/native_module_tests.cpp)
target_compile_features(hgl_native_module_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_native_module_tests PRIVATE hgl::driver Catch2::Catch2WithMain)
hgl_link_runtime_executable(hgl_native_module_tests)
hgl_apply_warnings(hgl_native_module_tests)

add_test(NAME hgraph_language_native_module COMMAND hgl_native_module_tests)

# The installed lifecycle boundary is a C ABI rather than an accidental C++
# hgraph type contract. Compile and execute one provider-shaped table as C11.
add_executable(hgl_native_module_abi_c driver/native_module_abi.c)
set_target_properties(hgl_native_module_abi_c PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
target_link_libraries(hgl_native_module_abi_c PRIVATE hgl::native_interface)
if(MSVC)
    target_compile_options(hgl_native_module_abi_c PRIVATE /W4)
    if(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS)
        target_compile_options(hgl_native_module_abi_c PRIVATE /WX)
    endif()
else()
    target_compile_options(hgl_native_module_abi_c PRIVATE -Wall -Wextra -Wpedantic)
    if(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS)
        target_compile_options(hgl_native_module_abi_c PRIVATE -Werror)
    endif()
endif()

add_test(NAME hgraph_language_native_module_abi_c COMMAND hgl_native_module_abi_c)

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
hgl_add_module(hgl_codegen_fixture STATIC HGL
    codegen/parity.hgl
    codegen/runtime.hgl
    ../examples/conditional-result.hgl
    ../examples/conditional-sinks.hgl
    ../examples/dynamic-collection-iteration.hgl
    ../examples/fixed-list-iteration.hgl)
hgl_apply_warnings(hgl_codegen_fixture)
hgl_add_module(hgl_structural_fixture STATIC HGL ../examples/structural-types.hgl)
hgl_apply_warnings(hgl_structural_fixture)
hgl_add_module(hgl_generic_fixture STATIC HGL ../examples/operators-and-generics.hgl)
hgl_apply_warnings(hgl_generic_fixture)
hgl_add_module(hgl_collection_fixture STATIC HGL ../examples/collection-views.hgl)
hgl_apply_warnings(hgl_collection_fixture)
hgl_add_module(hgl_stateful_fixture STATIC HGL ../examples/stateful-node.hgl)
hgl_apply_warnings(hgl_stateful_fixture)
hgl_add_module(hgl_reference_fixture STATIC HGL ../examples/reference-routing.hgl)
hgl_apply_warnings(hgl_reference_fixture)
add_library(hgl_native_scalar_dependency INTERFACE)
add_library(checks::native_dependency ALIAS hgl_native_scalar_dependency)
target_include_directories(hgl_native_scalar_dependency INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/driver/include")
set_property(TARGET hgl_native_scalar_dependency PROPERTY HGL_MODULE_DESCRIPTORS "${_hgl_native_scalar_descriptor}")
hgl_add_module(hgl_native_scalar_import_fixture STATIC
    HGL driver/native-scalar-import.hgl
    LINK_LIBRARIES hgl_native_scalar_dependency
)
hgl_apply_warnings(hgl_native_scalar_import_fixture)

set(_hgl_generated_test_sources
    codegen/generated_tests.cpp
    codegen/generated_runtime_tests.cpp
    codegen/generated_native_tests.cpp
    codegen/generated_iteration_tests.cpp
    codegen/generated_reference_tests.cpp
    codegen/generated_structural_tests.cpp
    codegen/generated_generic_tests.cpp
)
set(_hgl_generated_test_libraries
    hgl_codegen_fixture
    hgl_structural_fixture
    hgl_generic_fixture
    hgl_collection_fixture
    hgl_stateful_fixture
    hgl_reference_fixture
    hgl_native_scalar_import_fixture
)
if(TARGET hgraph::analytics)
    # These two examples intentionally exercise an imported extension. Keep
    # the core language build usable when that independent extension is off.
    hgl_add_module(hgl_basic_examples_fixture STATIC
        HGL
            ../examples/midpoint.hgl
            ../examples/runtime-choice.hgl
        LINK_LIBRARIES
            hgraph::analytics
    )
    hgl_apply_warnings(hgl_basic_examples_fixture)
    list(APPEND _hgl_generated_test_sources codegen/generated_example_tests.cpp)
    list(APPEND _hgl_generated_test_libraries hgl_basic_examples_fixture)
endif()

add_executable(hgl_generated_tests ${_hgl_generated_test_sources})
target_compile_features(hgl_generated_tests PRIVATE cxx_std_23)
target_link_libraries(hgl_generated_tests PRIVATE
    ${_hgl_generated_test_libraries}
    hgl::wiring
    Catch2::Catch2WithMain
)
hgl_link_runtime_executable(hgl_generated_tests)
hgl_apply_warnings(hgl_generated_tests)

add_test(NAME hgraph_language_generated COMMAND hgl_generated_tests)
unset(_hgl_generated_test_sources)
unset(_hgl_generated_test_libraries)

add_test(
    NAME hgraph_language_test_parity
    COMMAND $<TARGET_FILE:hgl> test ${CMAKE_CURRENT_SOURCE_DIR}/codegen/parity.hgl
)
set_tests_properties(hgraph_language_test_parity PROPERTIES
    PASS_REGULAR_EXPRESSION "7 tests, 0 failed"
)

if(UNIX)
    # Runtime-node modules take the scripted compile/load path, then use the
    # same direct harness as composition-only tests to wire the registered
    # candidates. The prototype loader is currently Unix-only.
    add_test(
        NAME hgraph_language_test_runtime
        COMMAND $<TARGET_FILE:hgl> test ${CMAKE_CURRENT_SOURCE_DIR}/codegen/runtime.hgl
    )
    set_tests_properties(hgraph_language_test_runtime PROPERTIES
        PASS_REGULAR_EXPRESSION "3 tests, 0 failed"
    )

    add_test(
        NAME hgraph_language_run_runtime
        COMMAND $<TARGET_FILE:hgl> run ${CMAKE_CURRENT_SOURCE_DIR}/codegen/runtime.hgl
            --entry configured_app --set value=2.0 --set initial=5.0 --end 1us
    )
    set_tests_properties(hgraph_language_run_runtime PROPERTIES
        PASS_REGULAR_EXPRESSION "1970-01-01T00:00:00.000001Z 7"
    )
    set_tests_properties(
        hgraph_language_test_runtime
        hgraph_language_run_runtime
        PROPERTIES ENVIRONMENT "HGL_CACHE_DIR=${CMAKE_CURRENT_BINARY_DIR}/scripted-cache"
    )

    add_test(
        NAME hgraph_language_native_compile_failure
        COMMAND ${CMAKE_COMMAND}
            -DHGL=$<TARGET_FILE:hgl>
            -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/codegen/runtime.hgl
            -DOUT=${CMAKE_CURRENT_BINARY_DIR}/native-failure
            -P ${CMAKE_CURRENT_SOURCE_DIR}/driver/native_failure.cmake
    )

    add_test(
        NAME hgraph_language_native_cache
        COMMAND ${CMAKE_COMMAND}
            -DHGL=$<TARGET_FILE:hgl>
            -DCXX=${CMAKE_CXX_COMPILER}
            -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/codegen/runtime.hgl
            -DOUT=${CMAKE_CURRENT_BINARY_DIR}/native-cache
            -P ${CMAKE_CURRENT_SOURCE_DIR}/driver/native_cache.cmake
    )
endif()

# The command itself: representative composition and runtime modules emit.
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
        -DNATIVE_DESCRIPTOR=${_hgl_native_scalar_descriptor}
        "-DGENERATOR=${CMAKE_GENERATOR}"
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/package_test.cmake
)
set_tests_properties(hgraph_language_cmake_package PROPERTIES FIXTURES_REQUIRED hgl_native_scalar_descriptor)

# The guide's test example passes end to end through the driver
# (user guide, "Testing and running").
if(TARGET hgraph::analytics)
    add_test(
        NAME hgraph_language_test_midpoint
        COMMAND $<TARGET_FILE:hgl> test ${CMAKE_CURRENT_SOURCE_DIR}/../examples/midpoint.hgl
    )
    set_tests_properties(hgraph_language_test_midpoint PROPERTIES
        PASS_REGULAR_EXPRESSION "midpoint_ticks \\.\\.\\. ok"
    )
endif()
