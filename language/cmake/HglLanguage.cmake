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
# header/source pair named after it (`prices.hgl` -> `prices.h`, `prices.cpp`)
# whose namespace is the module name. The pair is compiled together with any
# hand-written SOURCES into one library that links `hgraph::core`, so a
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

include_guard(GLOBAL)

set(_HGL_LANGUAGE_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}")

# Keep module namespace spelling in lockstep with emit-cpp's generated C++.
# HGL identifiers are broader than C++ identifiers (`module prices.new` is
# valid), while the generated code also reserves a few implementation names.
function(_hgl_cpp_name out_var name)
    set(_reserved
        alignas alignof and and_eq asm auto bitand bitor bool break case catch char char8_t char16_t char32_t class
        compl concept const consteval constexpr constinit const_cast continue co_await co_return co_yield decltype default
        delete do double dynamic_cast else enum explicit export extern false float for friend goto if inline int long mutable
        namespace new noexcept not not_eq nullptr operator or or_eq private protected public register reinterpret_cast requires
        return short signed sizeof static static_assert static_cast struct switch template this thread_local throw true try
        typedef typeid typename union unsigned using virtual void volatile wchar_t while xor xor_eq
        w hgraph std operators operator_contracts register_operators compose name defaults)
    list(FIND _reserved "${name}" _reserved_index)
    if(_reserved_index EQUAL -1)
        set(${out_var} "${name}" PARENT_SCOPE)
    else()
        set(${out_var} "${name}_" PARENT_SCOPE)
    endif()
endfunction()

function(_hgl_module_namespace out_var source)
    file(STRINGS "${source}" _module_lines
        REGEX "^[ \t]*module[ \t]+[A-Za-z_][A-Za-z0-9_.]*")
    list(LENGTH _module_lines _module_count)
    if(NOT _module_count EQUAL 1)
        message(FATAL_ERROR
            "hgl_add_module: '${source}' must contain exactly one module declaration")
    endif()
    list(GET _module_lines 0 _module_line)
    string(REGEX REPLACE
        "^[ \t]*module[ \t]+([A-Za-z_][A-Za-z0-9_.]*).*$" "\\1"
        _module_name "${_module_line}")
    string(REPLACE "." ";" _module_parts "${_module_name}")
    set(_cpp_parts)
    foreach(_part IN LISTS _module_parts)
        _hgl_cpp_name(_cpp_part "${_part}")
        list(APPEND _cpp_parts "${_cpp_part}")
    endforeach()
    list(JOIN _cpp_parts "::" _module_namespace)
    set(${out_var} "${_module_namespace}" PARENT_SCOPE)
endfunction()

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
        if(NOT _hgl_PYTHON_MODULE MATCHES "^[A-Za-z_][A-Za-z0-9_]*$")
            message(FATAL_ERROR
                "hgl_add_module(${target}): PYTHON_MODULE must be one Python identifier")
        endif()
        set(_python_keywords
            False None True and as assert async await break class continue def del elif else except finally for from global if
            import in is lambda nonlocal not or pass raise return try while with yield)
        list(FIND _python_keywords "${_hgl_PYTHON_MODULE}" _python_keyword_index)
        if(NOT _python_keyword_index EQUAL -1)
            message(FATAL_ERROR
                "hgl_add_module(${target}): PYTHON_MODULE cannot be the Python keyword '${_hgl_PYTHON_MODULE}'")
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
    set(_generated_python)
    set(_generated_stems)
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
        set(_outputs "${_header}" "${_source}")
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
            DEPENDS "${_hgl_abs}" ${_hgl_compiler_dependency}
            COMMENT "hgl emit-cpp ${_stem}.hgl"
            VERBATIM
        )
        list(APPEND _generated_headers "${_header}")
        list(APPEND _generated_sources "${_source}")
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
    set_target_properties(${target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    set_source_files_properties(${_generated_headers} PROPERTIES HEADER_FILE_ONLY ON)

    if(_hgl_PYTHON_MODULE)
        # One registration call per HGL module, in HGL source order.
        set(_includes)
        set(_registrations)
        foreach(_hgl_file IN LISTS _hgl_HGL)
            get_filename_component(_hgl_abs "${_hgl_file}" ABSOLUTE)
            get_filename_component(_stem "${_hgl_abs}" NAME_WE)
            string(APPEND _includes "#include <${_stem}.h>\n")
            # Read the module declaration at configure time because this
            # bootstrap has to reference every generated registration function.
            _hgl_module_namespace(_module_ns "${_hgl_abs}")
            string(APPEND _registrations "    ${_module_ns}::register_operators();\n")
        endforeach()
        set(HGL_PYTHON_MODULE "${_hgl_PYTHON_MODULE}")
        set(HGL_PYTHON_INCLUDES "${_includes}")
        set(HGL_PYTHON_REGISTRATIONS "${_registrations}")
        set(_python_module_source "${CMAKE_CURRENT_BINARY_DIR}/hgl/${target}/${_hgl_PYTHON_MODULE}_module.cpp")
        configure_file("${_HGL_LANGUAGE_CMAKE_DIR}/hgl_python_module.cpp.in" "${_python_module_source}" @ONLY)

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
