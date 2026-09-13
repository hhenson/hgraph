if(NOT HGL OR NOT SOURCE OR NOT OUT OR NOT DESCRIPTOR)
    message(FATAL_ERROR "HGL, SOURCE, OUT and DESCRIPTOR are required")
endif()

file(MAKE_DIRECTORY "${OUT}")

# An imported native callable can be present without any production native
# code. In that case neither compilation nor its SDK dependencies may leak.
set(_native_source "${SOURCE}/native-only.hgl" --module-descriptor "${DESCRIPTOR}")
execute_process(COMMAND "${CMAKE_COMMAND}" -E env "HGL_CXX=${OUT}/missing-cxx"
    "${HGL}" run ${_native_source} --entry configured --set value=7 --end 1us
    RESULT_VARIABLE _status OUTPUT_VARIABLE _output ERROR_VARIABLE _errors)
if(NOT _status EQUAL 0 OR NOT _output MATCHES "1970-01-01T00:00:00\\.000001Z 7")
    message(FATAL_ERROR "production run loaded test-only native dependencies:\n${_output}\n${_errors}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env "HGL_CXX=${OUT}/missing-cxx"
    "HGL_ARTIFACT_DIR=${OUT}" "HGL_CACHE_DIR=${OUT}/native-cache"
    "${HGL}" test ${_native_source}
    RESULT_VARIABLE _status OUTPUT_VARIABLE _output ERROR_VARIABLE _errors)
if(_status EQUAL 0 OR NOT _errors MATCHES "native compilation failed")
    message(FATAL_ERROR "test mode omitted its native compilation requirement:\n${_output}\n${_errors}")
endif()
execute_process(COMMAND "${HGL}" emit-cpp ${_native_source} --out-dir "${OUT}/native"
    RESULT_VARIABLE _status OUTPUT_VARIABLE _output ERROR_VARIABLE _errors)
if(NOT _status EQUAL 0)
    message(FATAL_ERROR "native dependency production emission failed:\n${_output}\n${_errors}")
endif()
foreach(_artifact IN ITEMS native-only.h native-only.cpp native-only.hgl-module.json)
    file(READ "${OUT}/native/${_artifact}" _content)
    if(_content MATCHES "native_dependency|checks_native_dependency")
        message(FATAL_ERROR "test-only native dependency leaked into ${_artifact}:\n${_content}")
    endif()
endforeach()

function(run_hgl result output)
    execute_process(COMMAND "${HGL}" ${ARGN}
        RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
    set(${result} "${_result}" PARENT_SCOPE)
    set(${output} "${_stdout}\n${_stderr}" PARENT_SCOPE)
endfunction()

file(MAKE_DIRECTORY "${OUT}/first" "${OUT}/second")
set(_parts "${SOURCE}/api.hgl" --part "${SOURCE}/helpers.hgl" --part "${SOURCE}/cases.hgl")
# Production composition must not require a compiler just because its tests
# contain runtime helpers. A missing compiler makes accidental loading fail.
execute_process(COMMAND "${CMAKE_COMMAND}" -E env "HGL_CXX=${OUT}/missing-cxx"
    "${HGL}" run ${_parts} --entry configured --set value=7 --end 1us
    RESULT_VARIABLE _status OUTPUT_VARIABLE _output ERROR_VARIABLE _errors)
if(NOT _status EQUAL 0 OR NOT _output MATCHES "1970-01-01T00:00:00\\.000001Z 7")
    message(FATAL_ERROR "production run loaded test helpers:\n${_output}\n${_errors}")
endif()
run_hgl(_status _output test ${_parts})
if(NOT _status EQUAL 0 OR NOT _output MATCHES "4 tests, 0 failed")
    message(FATAL_ERROR "module-wide test helper execution failed:\n${_output}")
endif()
run_hgl(_status _output test ${_parts} from_cases)
if(NOT _status EQUAL 0 OR NOT _output MATCHES "1 test, 0 failed")
    message(FATAL_ERROR "selecting a test from a context failed:\n${_output}")
endif()

run_hgl(_status _output emit-cpp ${_parts} --out-dir "${OUT}/first")
if(NOT _status EQUAL 0)
    message(FATAL_ERROR "production emission failed:\n${_output}")
endif()
run_hgl(_status _output emit-cpp "${SOURCE}/api.hgl" --part "${SOURCE}/cases.hgl"
    --part "${SOURCE}/helpers.hgl" --out-dir "${OUT}/second")
if(NOT _status EQUAL 0)
    message(FATAL_ERROR "reordered production emission failed:\n${_output}")
endif()
foreach(_artifact IN ITEMS api.h api.cpp api.hgl-module.json)
    file(READ "${OUT}/first/${_artifact}" _first)
    file(READ "${OUT}/second/${_artifact}" _second)
    if(NOT _first STREQUAL _second OR _first MATCHES "fixture_|\\$test|\\$lift")
        message(FATAL_ERROR "test helper leaked or part order affected ${_artifact}:\n${_first}")
    endif()
endforeach()

run_hgl(_status _output check ${_parts} --part "${SOURCE}/production-leak.hgl")
if(_status EQUAL 0 OR NOT _output MATCHES "fixture_node")
    message(FATAL_ERROR "production was allowed to reference test code:\n${_output}")
endif()
run_hgl(_status _output check "${SOURCE}/outside.hgl"
    --module-descriptor "${OUT}/first/api.hgl-module.json")
if(_status EQUAL 0 OR NOT _output MATCHES "does not export 'fixture_node'")
    message(FATAL_ERROR "another module was allowed to import a test helper:\n${_output}")
endif()

execute_process(COMMAND "${HGL}" repl INPUT_FILE "${SOURCE}/session.txt"
    RESULT_VARIABLE _status OUTPUT_VARIABLE _output ERROR_VARIABLE _errors)
string(REGEX MATCHALL "first \\.\\.\\. +ok" _first_runs "${_output}")
list(LENGTH _first_runs _first_count)
if(NOT _status EQUAL 0 OR NOT _first_count EQUAL 1 OR
   NOT _output MATCHES "second \\.\\.\\. +ok" OR NOT _output MATCHES "\\[5\\]")
    message(FATAL_ERROR "REPL test contexts failed or reran old tests:\n${_output}\n${_errors}")
endif()
