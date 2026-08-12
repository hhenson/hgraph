include(CMakeParseArguments)

function(hgraph_analytics_find_arrow)
    cmake_parse_arguments(PARSE_ARGV 0 _hgraph_analytics_arrow
        "PREFER_PYARROW" "" "")

    if(TARGET Arrow::arrow_shared AND
       TARGET ArrowCompute::arrow_compute_shared)
        return()
    endif()

    if(_hgraph_analytics_arrow_PREFER_PYARROW)
        if(NOT Python_EXECUTABLE)
            find_package(Python 3.12 COMPONENTS Interpreter REQUIRED)
        endif()
        set(_hgraph_analytics_pyarrow_probe [=[
import pathlib, pyarrow, sys
if int(pyarrow.__version__.split(".", 1)[0]) != 25:
    raise SystemExit(f"hgraph-analytics requires PyArrow 25, found {pyarrow.__version__}")
root = pathlib.Path(pyarrow.__file__).resolve().parent
def pick(pattern):
    matches = sorted(root.glob(pattern))
    if not matches:
        raise SystemExit(f"missing PyArrow library matching {pattern!r} under {root}")
    return matches[0]
if sys.platform == "win32":
    arrow_link, compute_link = pick("arrow.lib"), pick("arrow_compute.lib")
    arrow_runtime, compute_runtime = pick("arrow.dll"), pick("arrow_compute.dll")
else:
    arrow_link = arrow_runtime = pick("libarrow.*")
    compute_link = compute_runtime = pick("libarrow_compute.*")
for path in (root / "include", arrow_link, compute_link, arrow_runtime, compute_runtime):
    print(path.as_posix())
]=])
        execute_process(
            COMMAND "${Python_EXECUTABLE}" -c "${_hgraph_analytics_pyarrow_probe}"
            OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE _hgraph_analytics_pyarrow_info
            ERROR_VARIABLE _hgraph_analytics_pyarrow_error
            RESULT_VARIABLE _hgraph_analytics_pyarrow_result
        )
        if(NOT _hgraph_analytics_pyarrow_result EQUAL 0)
            message(FATAL_ERROR
                "hgraph-analytics requires a compatible PyArrow installation: "
                "${_hgraph_analytics_pyarrow_error}")
        endif()
        string(REPLACE "\n" ";" _hgraph_analytics_pyarrow_info
            "${_hgraph_analytics_pyarrow_info}")
        list(GET _hgraph_analytics_pyarrow_info 0 _hgraph_analytics_arrow_include)
        list(GET _hgraph_analytics_pyarrow_info 1 _hgraph_analytics_arrow_link)
        list(GET _hgraph_analytics_pyarrow_info 2 _hgraph_analytics_compute_link)
        list(GET _hgraph_analytics_pyarrow_info 3 _hgraph_analytics_arrow_runtime)
        list(GET _hgraph_analytics_pyarrow_info 4 _hgraph_analytics_compute_runtime)

        if(NOT EXISTS "${_hgraph_analytics_arrow_include}/arrow/api.h")
            message(FATAL_ERROR
                "PyArrow headers not found under ${_hgraph_analytics_arrow_include}")
        endif()

        if(NOT TARGET Arrow::arrow_shared)
            add_library(Arrow::arrow_shared SHARED IMPORTED GLOBAL)
            set_target_properties(Arrow::arrow_shared PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_analytics_arrow_include}")
            if(WIN32)
                set_target_properties(Arrow::arrow_shared PROPERTIES
                    IMPORTED_IMPLIB "${_hgraph_analytics_arrow_link}"
                    IMPORTED_LOCATION "${_hgraph_analytics_arrow_runtime}")
            else()
                set_target_properties(Arrow::arrow_shared PROPERTIES
                    IMPORTED_LOCATION "${_hgraph_analytics_arrow_link}")
            endif()
        endif()

        if(NOT TARGET ArrowCompute::arrow_compute_shared)
            add_library(ArrowCompute::arrow_compute_shared SHARED IMPORTED GLOBAL)
            set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_analytics_arrow_include}")
            if(WIN32)
                set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
                    IMPORTED_IMPLIB "${_hgraph_analytics_compute_link}"
                    IMPORTED_LOCATION "${_hgraph_analytics_compute_runtime}")
            else()
                set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
                    IMPORTED_LOCATION "${_hgraph_analytics_compute_link}")
            endif()
        endif()
        return()
    endif()

    find_package(Arrow CONFIG REQUIRED)
    if(NOT TARGET ArrowCompute::arrow_compute_shared AND
       NOT TARGET ArrowCompute::arrow_compute_static)
        find_package(ArrowCompute CONFIG REQUIRED)
    endif()
endfunction()
