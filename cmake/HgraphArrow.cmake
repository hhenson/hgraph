include_guard(GLOBAL)
# Arrow is too heavy to FetchContent by default. Explicit pyarrow-backed builds
# may use its bundled libraries; the resulting native targets do not link the
# Python runtime unless Python support is independently enabled.
set(HGRAPH_PYARROW_LIBRARY_DIR "" CACHE PATH "Directory containing pyarrow's bundled Arrow libraries")
set(HGRAPH_PYARROW_ABI_MAJOR "25" CACHE STRING "Required pyarrow/Arrow ABI major for pyarrow-backed builds")
if(HGRAPH_USE_PYARROW_ARROW)
    if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()
    find_package(Python 3.12 COMPONENTS Interpreter REQUIRED)
    set(_hgraph_pyarrow_probe [=[
import pathlib, pyarrow, sys
expected_major = int(sys.argv[1])
actual_major = int(pyarrow.__version__.split(".", 1)[0])
if actual_major != expected_major:
    raise SystemExit(
        f"pyarrow {pyarrow.__version__} provides Arrow ABI major {actual_major}; "
        f"this build requires major {expected_major}"
    )
root = pathlib.Path(pyarrow.__file__).resolve().parent
def pick(pattern):
    matches = sorted(root.glob(pattern))
    if not matches:
        raise SystemExit(f"missing pyarrow library matching {pattern!r} under {root}")
    return matches[0]
if sys.platform == "win32":
    arrow_link = pick("arrow.lib")
    compute_link = pick("arrow_compute.lib")
    acero_link = pick("arrow_acero.lib")
    arrow_runtime = pick("arrow.dll")
    compute_runtime = pick("arrow_compute.dll")
    acero_runtime = pick("arrow_acero.dll")
else:
    arrow_link = arrow_runtime = pick("libarrow.*")
    compute_link = compute_runtime = pick("libarrow_compute.*")
    acero_link = acero_runtime = pick("libarrow_acero.*")
for path in (root, root / "include", arrow_link, compute_link, acero_link,
             arrow_runtime, compute_runtime, acero_runtime):
    print(path.as_posix())
]=])
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -c "${_hgraph_pyarrow_probe}" "${HGRAPH_PYARROW_ABI_MAJOR}"
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE _hgraph_pyarrow_info
        ERROR_VARIABLE _hgraph_pyarrow_error
        RESULT_VARIABLE _hgraph_pyarrow_result
    )
    if(NOT _hgraph_pyarrow_result EQUAL 0)
        message(FATAL_ERROR
            "HGRAPH_USE_PYARROW_ARROW=ON requires a compatible pyarrow installation: "
            "${_hgraph_pyarrow_error}")
    endif()
    string(REPLACE "\n" ";" _hgraph_pyarrow_info "${_hgraph_pyarrow_info}")
    list(GET _hgraph_pyarrow_info 0 _hgraph_pyarrow_dir)
    list(GET _hgraph_pyarrow_info 1 _hgraph_pyarrow_include)
    list(GET _hgraph_pyarrow_info 2 _hgraph_pyarrow_arrow_lib)
    list(GET _hgraph_pyarrow_info 3 _hgraph_pyarrow_compute_lib)
    list(GET _hgraph_pyarrow_info 4 _hgraph_pyarrow_acero_lib)
    list(GET _hgraph_pyarrow_info 5 HGRAPH_PYARROW_ARROW_RUNTIME)
    list(GET _hgraph_pyarrow_info 6 HGRAPH_PYARROW_COMPUTE_RUNTIME)
    list(GET _hgraph_pyarrow_info 7 HGRAPH_PYARROW_ACERO_RUNTIME)
    if(NOT EXISTS "${_hgraph_pyarrow_include}/arrow/api.h")
        message(FATAL_ERROR "pyarrow headers not found under ${_hgraph_pyarrow_include}")
    endif()
    function(_hgraph_import_pyarrow_component namespace name link_library runtime_library)
        if(TARGET "${namespace}::${name}_shared" OR TARGET "${namespace}::${name}_static")
            return()
        endif()
        add_library("${namespace}::${name}_shared" SHARED IMPORTED GLOBAL)
        set_target_properties("${namespace}::${name}_shared" PROPERTIES
            IMPORTED_LOCATION "${runtime_library}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}")
        if(WIN32)
            set_target_properties("${namespace}::${name}_shared" PROPERTIES
                IMPORTED_IMPLIB "${link_library}")
        endif()
    endfunction()
    _hgraph_import_pyarrow_component(Arrow arrow
        "${_hgraph_pyarrow_arrow_lib}" "${HGRAPH_PYARROW_ARROW_RUNTIME}")
    _hgraph_import_pyarrow_component(ArrowCompute arrow_compute
        "${_hgraph_pyarrow_compute_lib}" "${HGRAPH_PYARROW_COMPUTE_RUNTIME}")
    _hgraph_import_pyarrow_component(ArrowAcero arrow_acero
        "${_hgraph_pyarrow_acero_lib}" "${HGRAPH_PYARROW_ACERO_RUNTIME}")
    set(HGRAPH_PYARROW_LIBRARY_DIR "${_hgraph_pyarrow_dir}" CACHE PATH
        "Directory containing pyarrow's bundled Arrow libraries" FORCE)
    set(HGRAPH_PYARROW_RUNTIME_DIRS "${_hgraph_pyarrow_dir}")
    if(WIN32)
        get_filename_component(_hgraph_pyarrow_site_packages
            "${_hgraph_pyarrow_dir}" DIRECTORY)
        list(APPEND HGRAPH_PYARROW_RUNTIME_DIRS
            "${_hgraph_pyarrow_site_packages}")
        if(EXISTS "${_hgraph_pyarrow_site_packages}/pyarrow.libs")
            list(APPEND HGRAPH_PYARROW_RUNTIME_DIRS
                "${_hgraph_pyarrow_site_packages}/pyarrow.libs")
        endif()
    endif()
else()
    if(NOT TARGET Arrow::arrow_shared AND NOT TARGET Arrow::arrow_static)
        find_package(Arrow CONFIG REQUIRED)
    endif()
    # Recent Arrow releases split compute/acero into their own CMake packages; some
    # packagers (Conan's Arrow recipe) define all three target namespaces
    # from the single Arrow config, in which case the separate packages do
    # not exist and must not be required.
    if(NOT TARGET ArrowCompute::arrow_compute_shared AND
       NOT TARGET ArrowCompute::arrow_compute_static)
        find_package(ArrowCompute CONFIG REQUIRED)
    endif()
    if(NOT TARGET ArrowAcero::arrow_acero_shared AND
       NOT TARGET ArrowAcero::arrow_acero_static)
        find_package(ArrowAcero CONFIG REQUIRED)
    endif()
endif()
