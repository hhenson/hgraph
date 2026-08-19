include(CMakeParseArguments)

# Locate the Arrow runtime hgraph-persistence stores frames with, preferring
# pyarrow's bundled libraries for wheel builds (one Arrow runtime per
# process). Sets in the caller scope:
#   HGRAPH_PERSISTENCE_WITH_PARQUET  ON when a parquet library + headers exist
#   HGRAPH_PERSISTENCE_WITH_S3       ON when arrow/filesystem/s3fs.h exists
# and defines Arrow::arrow_shared (and Parquet::parquet_shared when found).
# Locate a Parquet runtime BESIDE ``arrow_target``'s own library, and define
# ``Parquet::parquet_shared`` from it. Parquet must always come from the same
# installation as the Arrow it is used with; ``find_package(Parquet)`` would
# happily return a system Parquet beside a pyarrow Arrow, which is two Arrow
# runtimes in one process.
function(hgraph_persistence_arrow_sibling_parquet arrow_target out_var)
    set(${out_var} OFF PARENT_SCOPE)
    if(NOT TARGET ${arrow_target})
        return()
    endif()

    set(_hgraph_persistence_arrow_lib "")
    foreach(_hgraph_persistence_property
            IMPORTED_LOCATION IMPORTED_LOCATION_RELEASE
            IMPORTED_LOCATION_RELWITHDEBINFO IMPORTED_LOCATION_DEBUG
            IMPORTED_IMPLIB IMPORTED_IMPLIB_RELEASE)
        get_target_property(_hgraph_persistence_value
            ${arrow_target} ${_hgraph_persistence_property})
        if(_hgraph_persistence_value)
            set(_hgraph_persistence_arrow_lib "${_hgraph_persistence_value}")
            break()
        endif()
    endforeach()
    if(NOT _hgraph_persistence_arrow_lib)
        return()
    endif()

    get_filename_component(_hgraph_persistence_arrow_dir
        "${_hgraph_persistence_arrow_lib}" DIRECTORY)
    file(GLOB _hgraph_persistence_parquet_runtime LIST_DIRECTORIES false
        "${_hgraph_persistence_arrow_dir}/libparquet.*${CMAKE_SHARED_LIBRARY_SUFFIX}"
        "${_hgraph_persistence_arrow_dir}/parquet.dll")
    if(NOT _hgraph_persistence_parquet_runtime)
        return()
    endif()
    list(GET _hgraph_persistence_parquet_runtime 0 _hgraph_persistence_parquet_lib)

    get_target_property(_hgraph_persistence_arrow_includes
        ${arrow_target} INTERFACE_INCLUDE_DIRECTORIES)
    set(_hgraph_persistence_parquet_headers OFF)
    foreach(_hgraph_persistence_dir IN LISTS _hgraph_persistence_arrow_includes)
        if(EXISTS "${_hgraph_persistence_dir}/parquet/arrow/writer.h")
            set(_hgraph_persistence_parquet_headers ON)
        endif()
    endforeach()
    if(NOT _hgraph_persistence_parquet_headers)
        return()
    endif()

    if(NOT TARGET Parquet::parquet_shared)
        add_library(Parquet::parquet_shared SHARED IMPORTED GLOBAL)
        set_target_properties(Parquet::parquet_shared PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_persistence_arrow_includes}")
        if(WIN32)
            file(GLOB _hgraph_persistence_parquet_implib LIST_DIRECTORIES false
                "${_hgraph_persistence_arrow_dir}/parquet.lib"
                "${_hgraph_persistence_arrow_dir}/libparquet.lib")
            if(_hgraph_persistence_parquet_implib)
                list(GET _hgraph_persistence_parquet_implib 0
                    _hgraph_persistence_parquet_implib_path)
                set_target_properties(Parquet::parquet_shared PROPERTIES
                    IMPORTED_IMPLIB "${_hgraph_persistence_parquet_implib_path}"
                    IMPORTED_LOCATION "${_hgraph_persistence_parquet_lib}")
            endif()
        else()
            set_target_properties(Parquet::parquet_shared PROPERTIES
                IMPORTED_LOCATION "${_hgraph_persistence_parquet_lib}")
        endif()
    endif()
    set(${out_var} ON PARENT_SCOPE)
endfunction()

# Set ``out_var`` to ON when ``arrow_target``'s include directories carry the
# S3 filesystem header. Probing the RESOLVED target keeps the answer tied to
# the Arrow installation actually being linked.
function(hgraph_persistence_arrow_has_s3 arrow_target out_var)
    set(${out_var} OFF PARENT_SCOPE)
    if(NOT TARGET ${arrow_target})
        return()
    endif()
    get_target_property(_hgraph_persistence_arrow_includes
        ${arrow_target} INTERFACE_INCLUDE_DIRECTORIES)
    if(NOT _hgraph_persistence_arrow_includes)
        return()
    endif()
    foreach(_hgraph_persistence_dir IN LISTS _hgraph_persistence_arrow_includes)
        if(EXISTS "${_hgraph_persistence_dir}/arrow/filesystem/s3fs.h")
            set(${out_var} ON PARENT_SCOPE)
            return()
        endif()
    endforeach()
endfunction()

function(hgraph_persistence_find_arrow)
    cmake_parse_arguments(PARSE_ARGV 0 _hgraph_persistence_arrow
        "PREFER_PYARROW" "" "")

    # A parent build may already have resolved the Arrow VALUE runtime (core
    # links arrow/compute/acero for its Frame value kind), and a system Arrow
    # may provide only the static target.
    set(_hgraph_persistence_arrow_target "")
    if(TARGET Arrow::arrow_shared)
        set(_hgraph_persistence_arrow_target Arrow::arrow_shared)
    elseif(TARGET Arrow::arrow_static)
        set(_hgraph_persistence_arrow_target Arrow::arrow_static)
    endif()

    if(_hgraph_persistence_arrow_target)
        # Parquet and S3 are DURABLE STORE policy, decided here rather than
        # inherited from the parent's feature answers (RFC 0025 checkpoint 5).
        # They must nonetheless come from the SAME Arrow installation the
        # parent resolved: pyarrow's Parquet beside a system Arrow would put
        # two Arrow runtimes in one process, which is exactly what this
        # extension exists to avoid.
        if(TARGET Parquet::parquet_shared OR TARGET Parquet::parquet_static)
            # The parent resolved Parquet too; it is already Arrow's sibling.
            set(HGRAPH_PERSISTENCE_WITH_PARQUET ON PARENT_SCOPE)
        else()
            hgraph_persistence_arrow_sibling_parquet(
                "${_hgraph_persistence_arrow_target}" _hgraph_persistence_parquet_found)
            set(HGRAPH_PERSISTENCE_WITH_PARQUET
                "${_hgraph_persistence_parquet_found}" PARENT_SCOPE)
        endif()
        hgraph_persistence_arrow_has_s3("${_hgraph_persistence_arrow_target}"
            _hgraph_persistence_s3_found)
        set(HGRAPH_PERSISTENCE_WITH_S3 "${_hgraph_persistence_s3_found}" PARENT_SCOPE)
        return()
    endif()

    if(_hgraph_persistence_arrow_PREFER_PYARROW)
        if(NOT Python_EXECUTABLE)
            find_package(Python 3.12 COMPONENTS Interpreter REQUIRED)
        endif()
        set(_hgraph_persistence_pyarrow_probe [=[
import pathlib, pyarrow, sys
if int(pyarrow.__version__.split(".", 1)[0]) != 25:
    raise SystemExit(f"hgraph-persistence requires PyArrow 25, found {pyarrow.__version__}")
root = pathlib.Path(pyarrow.__file__).resolve().parent
def pick(pattern):
    matches = sorted(root.glob(pattern))
    if not matches:
        raise SystemExit(f"missing PyArrow library matching {pattern!r} under {root}")
    return matches[0]
def maybe(pattern):
    matches = sorted(root.glob(pattern))
    return matches[0] if matches else ""
if sys.platform == "win32":
    arrow_link, arrow_runtime = pick("arrow.lib"), pick("arrow.dll")
    parquet_link, parquet_runtime = maybe("parquet.lib"), maybe("parquet.dll")
else:
    arrow_link = arrow_runtime = pick("libarrow.*")
    parquet_link = parquet_runtime = maybe("libparquet.*")
include = root / "include"
s3 = "1" if (include / "arrow" / "filesystem" / "s3fs.h").exists() else "0"
parquet_headers = (include / "parquet" / "arrow" / "writer.h").exists()
parquet = "1" if (parquet_link and parquet_headers) else "0"
for line in (include.as_posix(), arrow_link if isinstance(arrow_link, str) else arrow_link.as_posix(),
             arrow_runtime if isinstance(arrow_runtime, str) else arrow_runtime.as_posix(),
             (parquet_link.as_posix() if parquet_link else ""),
             (parquet_runtime.as_posix() if parquet_runtime else ""),
             parquet, s3):
    print(line)
]=])
        execute_process(
            COMMAND "${Python_EXECUTABLE}" -c "${_hgraph_persistence_pyarrow_probe}"
            OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE _hgraph_persistence_pyarrow_info
            ERROR_VARIABLE _hgraph_persistence_pyarrow_error
            RESULT_VARIABLE _hgraph_persistence_pyarrow_result
        )
        if(NOT _hgraph_persistence_pyarrow_result EQUAL 0)
            message(FATAL_ERROR
                "hgraph-persistence requires a compatible PyArrow installation: "
                "${_hgraph_persistence_pyarrow_error}")
        endif()
        string(REPLACE "\n" ";" _hgraph_persistence_pyarrow_info
            "${_hgraph_persistence_pyarrow_info}")
        list(GET _hgraph_persistence_pyarrow_info 0 _hgraph_persistence_arrow_include)
        list(GET _hgraph_persistence_pyarrow_info 1 _hgraph_persistence_arrow_link)
        list(GET _hgraph_persistence_pyarrow_info 2 _hgraph_persistence_arrow_runtime)
        list(GET _hgraph_persistence_pyarrow_info 3 _hgraph_persistence_parquet_link)
        list(GET _hgraph_persistence_pyarrow_info 4 _hgraph_persistence_parquet_runtime)
        list(GET _hgraph_persistence_pyarrow_info 5 _hgraph_persistence_parquet)
        list(GET _hgraph_persistence_pyarrow_info 6 _hgraph_persistence_s3)

        if(NOT EXISTS "${_hgraph_persistence_arrow_include}/arrow/api.h")
            message(FATAL_ERROR
                "PyArrow headers not found under ${_hgraph_persistence_arrow_include}")
        endif()

        add_library(Arrow::arrow_shared SHARED IMPORTED GLOBAL)
        set_target_properties(Arrow::arrow_shared PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_persistence_arrow_include}")
        if(WIN32)
            set_target_properties(Arrow::arrow_shared PROPERTIES
                IMPORTED_IMPLIB "${_hgraph_persistence_arrow_link}"
                IMPORTED_LOCATION "${_hgraph_persistence_arrow_runtime}")
        else()
            set_target_properties(Arrow::arrow_shared PROPERTIES
                IMPORTED_LOCATION "${_hgraph_persistence_arrow_link}")
        endif()

        if(_hgraph_persistence_parquet STREQUAL "1" AND NOT TARGET Parquet::parquet_shared)
            add_library(Parquet::parquet_shared SHARED IMPORTED GLOBAL)
            set_target_properties(Parquet::parquet_shared PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_persistence_arrow_include}")
            if(WIN32)
                set_target_properties(Parquet::parquet_shared PROPERTIES
                    IMPORTED_IMPLIB "${_hgraph_persistence_parquet_link}"
                    IMPORTED_LOCATION "${_hgraph_persistence_parquet_runtime}")
            else()
                set_target_properties(Parquet::parquet_shared PROPERTIES
                    IMPORTED_LOCATION "${_hgraph_persistence_parquet_link}")
            endif()
        endif()

        if(_hgraph_persistence_parquet STREQUAL "1")
            set(HGRAPH_PERSISTENCE_WITH_PARQUET ON PARENT_SCOPE)
        else()
            set(HGRAPH_PERSISTENCE_WITH_PARQUET OFF PARENT_SCOPE)
        endif()
        if(_hgraph_persistence_s3 STREQUAL "1")
            set(HGRAPH_PERSISTENCE_WITH_S3 ON PARENT_SCOPE)
        else()
            set(HGRAPH_PERSISTENCE_WITH_S3 OFF PARENT_SCOPE)
        endif()
        return()
    endif()

    find_package(Arrow CONFIG REQUIRED)
    find_package(Parquet CONFIG QUIET)
    if(TARGET Parquet::parquet_shared OR TARGET Parquet::parquet_static)
        set(HGRAPH_PERSISTENCE_WITH_PARQUET ON PARENT_SCOPE)
    else()
        set(HGRAPH_PERSISTENCE_WITH_PARQUET OFF PARENT_SCOPE)
    endif()
    # A system Arrow may ship only the static target.
    if(TARGET Arrow::arrow_shared)
        set(_hgraph_persistence_arrow_target Arrow::arrow_shared)
    else()
        set(_hgraph_persistence_arrow_target Arrow::arrow_static)
    endif()
    hgraph_persistence_arrow_has_s3("${_hgraph_persistence_arrow_target}"
        _hgraph_persistence_s3_found)
    set(HGRAPH_PERSISTENCE_WITH_S3 "${_hgraph_persistence_s3_found}" PARENT_SCOPE)
endfunction()
