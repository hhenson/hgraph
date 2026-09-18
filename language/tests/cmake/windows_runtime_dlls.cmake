if(NOT BUILD OR NOT HGL OR NOT OUT OR NOT BINDIR)
    message(FATAL_ERROR "BUILD, HGL, OUT and BINDIR are required")
endif()

# Single-config generators may have no CMAKE_BUILD_TYPE.
set(_config_args)
if(DEFINED CONFIG AND NOT "${CONFIG}" STREQUAL "")
    list(APPEND _config_args --config "${CONFIG}")
endif()

# Do not inherit developer DLL directories. Both copies of the compiler must
# find their non-system dependencies beside the executable.
set(_system_path "$ENV{SystemRoot}/System32;$ENV{SystemRoot}")
function(check_compiler executable)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "PATH=${_system_path}" "${executable}" --version
        RESULT_VARIABLE _result OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
    if(NOT _result EQUAL 0 OR NOT _out MATCHES "hgl [0-9]")
        message(FATAL_ERROR "compiler failed without developer DLL paths: ${executable}\n${_result}\n${_out}\n${_err}")
    endif()
endfunction()

check_compiler("${HGL}")
file(REMOVE_RECURSE "${OUT}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${BUILD}" --prefix "${OUT}"
        ${_config_args} --component Runtime
    RESULT_VARIABLE _result OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
if(NOT _result EQUAL 0)
    message(FATAL_ERROR "compiler runtime install failed:\n${_out}\n${_err}")
endif()
check_compiler("${OUT}/${BINDIR}/hgl.exe")
