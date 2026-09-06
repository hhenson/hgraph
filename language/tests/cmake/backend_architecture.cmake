if(NOT DEFINED HGL_LANGUAGE_SOURCE_DIR)
    message(FATAL_ERROR "HGL_LANGUAGE_SOURCE_DIR is required")
endif()

file(GLOB_RECURSE _codegen_sources LIST_DIRECTORIES FALSE
    "${HGL_LANGUAGE_SOURCE_DIR}/codegen/*.h"
    "${HGL_LANGUAGE_SOURCE_DIR}/codegen/*.cpp"
)
set(_backend_sources
    ${_codegen_sources}
    "${HGL_LANGUAGE_SOURCE_DIR}/wiring/backend.h"
    "${HGL_LANGUAGE_SOURCE_DIR}/wiring/backend.cpp"
    "${HGL_LANGUAGE_SOURCE_DIR}/wiring/type_bridge.h"
    "${HGL_LANGUAGE_SOURCE_DIR}/wiring/type_bridge.cpp"
)

foreach(_source IN LISTS _backend_sources)
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
endforeach()
