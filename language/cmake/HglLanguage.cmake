# HglLanguage.cmake — build hgraph packages from HGL sources.
#
#   hgl_add_module(<target>
#       HGL <file.hgl>...
#       [SOURCES <file.cpp>...]
#       [OUT_DIR <dir>] | [INCLUDE_DIR <dir> SRC_DIR <dir>]
#       [LINK_LIBRARIES <target>...]
#       [STATIC | SHARED]
#       [PYTHON_MODULE <name> [PYTHON_PACKAGE_DIR <dir>]])
#
# Every `.hgl` file is compiled by `hgl emit-cpp` at build time into a
# header/source pair and descriptor named after it (`prices.hgl` -> `prices.h`,
# `prices.cpp`, `prices.hgl-module.json`) whose namespace is the module name.
# The pair is compiled together with any hand-written SOURCES into one library
# that links `hgraph::core`, so a
# package mixes generated and native code freely (developer guide, "C++
# backend, first pass"; user guide, "Building a package").
#
# With PYTHON_MODULE the function also produces a stable-ABI nanobind module
# whose import registers the package's operators, plus one generated Python
# wrapper module per HGL source exposing the exported functions through
# `hgraph.operator_function`. PYTHON_PACKAGE_DIR (default
# `${CMAKE_CURRENT_BINARY_DIR}/python/<name>`) receives the wrappers; the
# native module is built beside them so `from . import <name>` works.
#
# The generated headers are public: `target_include_directories` publishes
# the include directory, so a consumer can `#include <prices.h>` and
# `wire<examples::prices::smooth>(w, ...)` directly.
#
# The `hgl` compiler is the `hgl` target when the language is part of the
# build, else the installed `hgl` program (set HGL_EXECUTABLE to override).

# Script mode. `hgl_add_module` runs this file with `cmake -P` at build time
# to write a package's Python bootstrap from the descriptors `emit-cpp`
# produced. Each descriptor carries its module's registration symbol
# (`pkg::new_::register_operators`), so the C++ spelling of a module
# namespace has exactly one source, the compiler; CMake never re-derives it
# from the `module` line.
if(CMAKE_SCRIPT_MODE_FILE AND HGL_BOOTSTRAP_OUT)
    set(HGL_PYTHON_MODULE "${HGL_BOOTSTRAP_MODULE}")
    set(HGL_PYTHON_INCLUDES "")
    set(HGL_PYTHON_REGISTRATIONS "")
    foreach(_header IN LISTS HGL_BOOTSTRAP_HEADERS)
        string(APPEND HGL_PYTHON_INCLUDES "#include <${_header}>\n")
    endforeach()
    foreach(_descriptor IN LISTS HGL_BOOTSTRAP_DESCRIPTORS)
        file(READ "${_descriptor}" _json)
        string(JSON _symbol ERROR_VARIABLE _json_error GET "${_json}" build registration symbol)
        if(_json_error OR NOT _symbol)
            message(FATAL_ERROR "hgl_add_module: '${_descriptor}' carries no registration symbol: ${_json_error}")
        endif()
        string(APPEND HGL_PYTHON_REGISTRATIONS "    ${_symbol}();\n")
    endforeach()
    configure_file("${HGL_BOOTSTRAP_TEMPLATE}" "${HGL_BOOTSTRAP_OUT}" @ONLY)
    return()
endif()

include_guard(GLOBAL)

set(_HGL_LANGUAGE_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}")
if(NOT TARGET hgl::native_interface AND EXISTS "${_HGL_LANGUAGE_CMAKE_DIR}/HglLanguageTargets.cmake")
    include("${_HGL_LANGUAGE_CMAKE_DIR}/HglLanguageTargets.cmake")
endif()

# The installed core-native target carries a generated descriptor beside this
# helper. Custom build-tree target properties are intentionally not exported;
# reconstruct the relocatable path when an installed consumer loads the SDK.
if(TARGET hgl::core_native)
    get_target_property(_hgl_core_native_descriptors hgl::core_native HGL_MODULE_DESCRIPTORS)
    if(NOT _hgl_core_native_descriptors OR _hgl_core_native_descriptors STREQUAL "_hgl_core_native_descriptors-NOTFOUND")
        set(_hgl_core_native_descriptor "${_HGL_LANGUAGE_CMAKE_DIR}/modules/native.hgl-module.json")
        if(EXISTS "${_hgl_core_native_descriptor}")
            set_property(TARGET hgl::core_native PROPERTY HGL_MODULE_DESCRIPTORS "${_hgl_core_native_descriptor}")
        endif()
        unset(_hgl_core_native_descriptor)
    endif()
    unset(_hgl_core_native_descriptors)
endif()

# The installed HGL-authored standard library is another generated module. Give
# downstream hgl_add_module() calls its relocated descriptor for the same reason
# as core_native above.
if(TARGET hgl::standard_library)
    get_target_property(_hgl_standard_library_descriptors hgl::standard_library HGL_MODULE_DESCRIPTORS)
    if(NOT _hgl_standard_library_descriptors OR
       _hgl_standard_library_descriptors STREQUAL "_hgl_standard_library_descriptors-NOTFOUND")
        set(_hgl_standard_library_descriptor "${_HGL_LANGUAGE_CMAKE_DIR}/modules/standard.hgl-module.json")
        if(EXISTS "${_hgl_standard_library_descriptor}")
            set_property(TARGET hgl::standard_library PROPERTY
                HGL_MODULE_DESCRIPTORS "${_hgl_standard_library_descriptor}")
        endif()
        unset(_hgl_standard_library_descriptor)
    endif()
    unset(_hgl_standard_library_descriptors)
endif()

function(_hgl_resolve_compiler out_var)
    if(TARGET hgl)
        set(${out_var} "$<TARGET_FILE:hgl>" PARENT_SCOPE)
        return()
    endif()
    if(HGL_EXECUTABLE)
        set(${out_var} "${HGL_EXECUTABLE}" PARENT_SCOPE)
        return()
    endif()
    find_program(HGL_EXECUTABLE hgl HINTS "${_HGL_LANGUAGE_CMAKE_DIR}/../../../bin")
    if(NOT HGL_EXECUTABLE)
        message(FATAL_ERROR "hgl_add_module: no `hgl` compiler; build the language or set HGL_EXECUTABLE")
    endif()
    set(${out_var} "${HGL_EXECUTABLE}" PARENT_SCOPE)
endfunction()

function(hgl_add_module target)
    cmake_parse_arguments(PARSE_ARGV 1 _hgl
        "STATIC;SHARED"
        "OUT_DIR;INCLUDE_DIR;SRC_DIR;PYTHON_MODULE;PYTHON_PACKAGE_DIR"
        "HGL;SOURCES;LINK_LIBRARIES")
    if(_hgl_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "hgl_add_module(${target}): unexpected arguments: ${_hgl_UNPARSED_ARGUMENTS}")
    endif()
    if(NOT _hgl_HGL)
        message(FATAL_ERROR "hgl_add_module(${target}): HGL needs at least one .hgl source")
    endif()
    if(_hgl_STATIC AND _hgl_SHARED)
        message(FATAL_ERROR "hgl_add_module(${target}): STATIC and SHARED are exclusive")
    endif()
    if(_hgl_OUT_DIR AND (_hgl_INCLUDE_DIR OR _hgl_SRC_DIR))
        message(FATAL_ERROR "hgl_add_module(${target}): use OUT_DIR or INCLUDE_DIR/SRC_DIR, not both")
    endif()
    if(_hgl_PYTHON_MODULE)
        # The shape check is early feedback; `emit-cpp --python-native` owns the
        # Python identifier and keyword rules and rejects the name at build time.
        if(NOT _hgl_PYTHON_MODULE MATCHES "^[A-Za-z_][A-Za-z0-9_]*$")
            message(FATAL_ERROR
                "hgl_add_module(${target}): PYTHON_MODULE must be one Python identifier")
        endif()
    endif()

    # Where the generated files go: one directory, or a split include/src pair.
    if(_hgl_OUT_DIR)
        set(_include_dir "${_hgl_OUT_DIR}")
        set(_src_dir "${_hgl_OUT_DIR}")
        set(_emit_placement --out-dir "${_hgl_OUT_DIR}")
    else()
        if(NOT _hgl_INCLUDE_DIR)
            set(_hgl_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/hgl/${target}/include")
        endif()
        if(NOT _hgl_SRC_DIR)
            set(_hgl_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/hgl/${target}/src")
        endif()
        set(_include_dir "${_hgl_INCLUDE_DIR}")
        set(_src_dir "${_hgl_SRC_DIR}")
        set(_emit_placement --include-dir "${_hgl_INCLUDE_DIR}" --src-dir "${_hgl_SRC_DIR}")
    endif()

    if(_hgl_PYTHON_MODULE AND NOT _hgl_PYTHON_PACKAGE_DIR)
        set(_hgl_PYTHON_PACKAGE_DIR "${CMAKE_CURRENT_BINARY_DIR}/python/${_hgl_PYTHON_MODULE}")
    endif()

    _hgl_resolve_compiler(_hgl_compiler)
    set(_hgl_compiler_dependency)
    if(TARGET hgl)
        set(_hgl_compiler_dependency hgl)
    else()
        # Installed consumers have no CMake target for the compiler. Its path
        # is still an input: replacing hgl must invalidate generated output.
        set(_hgl_compiler_dependency "${_hgl_compiler}")
    endif()

    set(_generated_headers)
    set(_generated_sources)
    set(_generated_descriptors)
    set(_generated_python)
    set(_generated_stems)
    set(_module_descriptor_options)
    set(_module_descriptor_dependencies)
    foreach(_dependency IN LISTS _hgl_LINK_LIBRARIES)
        if(NOT TARGET "${_dependency}")
            continue()
        endif()
        get_target_property(_dependency_descriptors "${_dependency}" HGL_MODULE_DESCRIPTORS)
        if(NOT _dependency_descriptors OR _dependency_descriptors STREQUAL "_dependency_descriptors-NOTFOUND")
            continue()
        endif()
        foreach(_dependency_descriptor IN LISTS _dependency_descriptors)
            list(APPEND _module_descriptor_options --module-descriptor "${_dependency_descriptor}")
            list(APPEND _module_descriptor_dependencies "${_dependency_descriptor}")
        endforeach()
    endforeach()
    foreach(_hgl_file IN LISTS _hgl_HGL)
        get_filename_component(_hgl_abs "${_hgl_file}" ABSOLUTE)
        get_filename_component(_stem "${_hgl_abs}" NAME_WE)
        list(FIND _generated_stems "${_stem}" _stem_index)
        if(NOT _stem_index EQUAL -1)
            message(FATAL_ERROR
                "hgl_add_module(${target}): HGL sources must have unique filename stems; '${_stem}' is repeated")
        endif()
        list(APPEND _generated_stems "${_stem}")
        set(_header "${_include_dir}/${_stem}.h")
        set(_source "${_src_dir}/${_stem}.cpp")
        set(_descriptor "${_src_dir}/${_stem}.hgl-module.json")
        set(_outputs "${_header}" "${_source}" "${_descriptor}")
        set(_python_options)
        if(_hgl_PYTHON_MODULE)
            set(_python "${_hgl_PYTHON_PACKAGE_DIR}/${_stem}.py")
            list(APPEND _outputs "${_python}")
            list(APPEND _generated_python "${_python}")
            set(_python_options --python "${_python}" --python-native "${_hgl_PYTHON_MODULE}")
        endif()
        add_custom_command(
            OUTPUT ${_outputs}
            COMMAND "${_hgl_compiler}" emit-cpp "${_hgl_abs}" ${_emit_placement} ${_python_options}
                    ${_module_descriptor_options}
            DEPENDS "${_hgl_abs}" ${_hgl_compiler_dependency} ${_module_descriptor_dependencies}
            COMMENT "hgl emit-cpp ${_stem}.hgl"
            VERBATIM
        )
        list(APPEND _generated_headers "${_header}")
        list(APPEND _generated_sources "${_source}")
        list(APPEND _generated_descriptors "${_descriptor}")
    endforeach()

    set(_kind)
    if(_hgl_STATIC)
        set(_kind STATIC)
    elseif(_hgl_SHARED)
        set(_kind SHARED)
    endif()
    add_library(${target} ${_kind} ${_generated_sources} ${_generated_headers} ${_hgl_SOURCES})
    target_compile_features(${target} PUBLIC cxx_std_23)
    target_include_directories(${target} PUBLIC "${_include_dir}")
    target_link_libraries(${target} PUBLIC hgraph::core ${_hgl_LINK_LIBRARIES})
    if(TARGET hgl::native_interface)
        target_link_libraries(${target} PUBLIC hgl::native_interface)
    endif()
    set_target_properties(${target} PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        # Source-native functions are ordinary generated C++ symbols consumed
        # by modules that link this target. Export them from Windows DLLs when
        # this target is SHARED (explicitly or through BUILD_SHARED_LIBS).
        WINDOWS_EXPORT_ALL_SYMBOLS ON)
    set_source_files_properties(${_generated_headers} PROPERTIES HEADER_FILE_ONLY ON)
    set_property(TARGET ${target} PROPERTY HGL_MODULE_DESCRIPTORS "${_generated_descriptors}")

    if(_hgl_PYTHON_MODULE)
        # One registration call per HGL module, in HGL source order. The
        # bootstrap is written at build time (this file in script mode) from
        # the descriptors emit-cpp produces: the compiler alone spells each
        # module's C++ namespace, so no table here can drift from it.
        set(_bootstrap_headers)
        foreach(_stem IN LISTS _generated_stems)
            list(APPEND _bootstrap_headers "${_stem}.h")
        endforeach()
        set(_python_module_source "${CMAKE_CURRENT_BINARY_DIR}/hgl/${target}/${_hgl_PYTHON_MODULE}_module.cpp")
        set(_bootstrap_template "${_HGL_LANGUAGE_CMAKE_DIR}/hgl_python_module.cpp.in")
        add_custom_command(
            OUTPUT "${_python_module_source}"
            COMMAND "${CMAKE_COMMAND}"
                "-DHGL_BOOTSTRAP_OUT=${_python_module_source}"
                "-DHGL_BOOTSTRAP_TEMPLATE=${_bootstrap_template}"
                "-DHGL_BOOTSTRAP_MODULE=${_hgl_PYTHON_MODULE}"
                "-DHGL_BOOTSTRAP_HEADERS=${_bootstrap_headers}"
                "-DHGL_BOOTSTRAP_DESCRIPTORS=${_generated_descriptors}"
                -P "${_HGL_LANGUAGE_CMAKE_DIR}/HglLanguage.cmake"
            DEPENDS ${_generated_descriptors} "${_bootstrap_template}" "${_HGL_LANGUAGE_CMAKE_DIR}/HglLanguage.cmake"
            COMMENT "hgl python bootstrap ${_hgl_PYTHON_MODULE}"
            VERBATIM
        )

        if(COMMAND hgraph_add_python_module AND TARGET hgraph::nanobind)
            hgraph_add_python_module(${_hgl_PYTHON_MODULE} STABLE_ABI NOMINSIZE "${_python_module_source}")
        elseif(COMMAND nanobind_add_module)
            nanobind_add_module(${_hgl_PYTHON_MODULE} STABLE_ABI NOMINSIZE NB_STATIC "${_python_module_source}")
        else()
            message(FATAL_ERROR
                "hgl_add_module(${target}): PYTHON_MODULE needs a Python-enabled hgraph SDK "
                "(hgraph_add_python_module) or nanobind (nanobind_add_module)")
        endif()
        target_link_libraries(${_hgl_PYTHON_MODULE} PRIVATE ${target})
        set_property(TARGET ${_hgl_PYTHON_MODULE} PROPERTY HGL_PYTHON_BOOTSTRAP "${_python_module_source}")
        # A generator expression suppresses the automatic Debug/Release child
        # directory that multi-config generators otherwise append. Wrappers,
        # __init__.py and the native extension therefore remain one package.
        set(_python_output_dir "$<1:${_hgl_PYTHON_PACKAGE_DIR}>")
        set_target_properties(${_hgl_PYTHON_MODULE} PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY "${_python_output_dir}"
            RUNTIME_OUTPUT_DIRECTORY "${_python_output_dir}")
        # The package's __init__ re-exports every wrapper module.
        set(_init_lines "\"\"\"${_hgl_PYTHON_MODULE}: hgraph operators generated from HGL by hgl_add_module.\"\"\"\n")
        set(_all_lines "\n__all__ = [\n")
        foreach(_python IN LISTS _generated_python)
            get_filename_component(_stem "${_python}" NAME_WE)
            string(APPEND _init_lines "from . import ${_stem} as _${_stem}\n")
            string(APPEND _init_lines "from .${_stem} import *  # noqa: F401,F403\n")
            string(APPEND _all_lines "    *_${_stem}.__all__,\n")
        endforeach()
        string(APPEND _init_lines "${_all_lines}]\n")
        file(GENERATE OUTPUT "${_hgl_PYTHON_PACKAGE_DIR}/__init__.py" CONTENT "${_init_lines}")
        add_custom_target(${target}_python_wrappers DEPENDS ${_generated_python})
        add_dependencies(${_hgl_PYTHON_MODULE} ${target}_python_wrappers)
    endif()
endfunction()
