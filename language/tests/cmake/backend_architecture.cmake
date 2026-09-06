cmake_minimum_required(VERSION 3.25)

if(NOT DEFINED HGL_LANGUAGE_SOURCE_DIR)
    message(FATAL_ERROR "HGL_LANGUAGE_SOURCE_DIR is required")
endif()

file(GLOB_RECURSE _backend_sources LIST_DIRECTORIES FALSE
    "${HGL_LANGUAGE_SOURCE_DIR}/codegen/*.h"
    "${HGL_LANGUAGE_SOURCE_DIR}/codegen/*.cpp"
    "${HGL_LANGUAGE_SOURCE_DIR}/descriptor/*.h"
    "${HGL_LANGUAGE_SOURCE_DIR}/descriptor/*.cpp"
    "${HGL_LANGUAGE_SOURCE_DIR}/wiring/*.h"
    "${HGL_LANGUAGE_SOURCE_DIR}/wiring/*.cpp"
)

set_property(GLOBAL PROPERTY HGL_BACKEND_ARCHITECTURE_SEEN "")

function(hgl_check_backend_source _source)
    get_filename_component(_source "${_source}" REALPATH)
    get_property(_seen GLOBAL PROPERTY HGL_BACKEND_ARCHITECTURE_SEEN)
    if(_source IN_LIST _seen)
        return()
    endif()
    list(APPEND _seen "${_source}")
    set_property(GLOBAL PROPERTY HGL_BACKEND_ARCHITECTURE_SEEN "${_seen}")

    file(READ "${_source}" _contents)
    foreach(_forbidden IN ITEMS
        "syntax/ast\\.h"
        "syntax/parser\\.h"
        "syntax/ast_projection\\.h"
        "semantics/resolve\\.h"
        "ResolvedModule"
        "syntax::ast"
    )
        if(_contents MATCHES "${_forbidden}")
            file(RELATIVE_PATH _relative "${HGL_LANGUAGE_SOURCE_DIR}" "${_source}")
            message(FATAL_ERROR "HGL execution backend ${_relative} contains forbidden frontend dependency ${_forbidden}")
        endif()
    endforeach()

    string(REGEX MATCHALL "#[ \t]*include[ \t]*[<\"][^>\"]+[>\"]" _includes "${_contents}")
    foreach(_directive IN LISTS _includes)
        string(REGEX REPLACE "^[^<\"]*[<\"]([^>\"]+)[>\"].*$" "\\1" _include "${_directive}")
        get_filename_component(_source_dir "${_source}" DIRECTORY)
        set(_dependency "${_source_dir}/${_include}")
        if(NOT EXISTS "${_dependency}")
            set(_dependency "${HGL_LANGUAGE_SOURCE_DIR}/${_include}")
        endif()
        if(EXISTS "${_dependency}")
            hgl_check_backend_source("${_dependency}")
        endif()
    endforeach()
endfunction()

foreach(_source IN LISTS _backend_sources)
    hgl_check_backend_source("${_source}")
endforeach()
