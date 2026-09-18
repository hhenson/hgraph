# Windows has no build rpath. Stage the linked DLLs, including PyArrow's
# dynamically loaded support libraries, beside each build-tree executable.
if(CMAKE_SCRIPT_MODE_FILE)
    foreach(_dll IN LISTS HGL_RUNTIME_DLLS)
        if(NOT _dll STREQUAL "")
            get_filename_component(_name "${_dll}" NAME)
            file(COPY_FILE "${_dll}" "${HGL_RUNTIME_DESTINATION}/${_name}" ONLY_IF_DIFFERENT)
        endif()
    endforeach()
    return()
endif()

include_guard(GLOBAL)

function(hgl_stage_runtime_dlls target)
    if(NOT WIN32)
        return()
    endif()
    set(_support_dlls)
    if(HGRAPH_USE_PYARROW_ARROW)
        file(GLOB _support_dlls LIST_DIRECTORIES false
            "${HGRAPH_PYARROW_LIBRARY_DIR}/../pyarrow.libs/*.dll")
    endif()
    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND "${CMAKE_COMMAND}"
            "-DHGL_RUNTIME_DLLS=$<TARGET_RUNTIME_DLLS:${target}>;${_support_dlls}"
            "-DHGL_RUNTIME_DESTINATION=$<TARGET_FILE_DIR:${target}>"
            -P "${CMAKE_CURRENT_FUNCTION_LIST_FILE}"
        COMMENT "Staging runtime DLLs beside ${target}"
        VERBATIM)
endfunction()
