if(NOT BUILD OR NOT HGL OR NOT OUT OR NOT BINDIR OR NOT CONFIG)
    message(FATAL_ERROR "BUILD, HGL, OUT, BINDIR and CONFIG are required")
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
        --config "${CONFIG}" --component Runtime
    RESULT_VARIABLE _result OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
if(NOT _result EQUAL 0)
    message(FATAL_ERROR "compiler runtime install failed:\n${_out}\n${_err}")
endif()
check_compiler("${OUT}/${BINDIR}/hgl.exe")
