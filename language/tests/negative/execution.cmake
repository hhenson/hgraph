cmake_minimum_required(VERSION 3.25)
file(MAKE_DIRECTORY "${OUT}")
function(run_case source status pattern)
    execute_process(COMMAND "${HGL}" test "${SOURCE}/${source}.hgl" ${ARGN}
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE errors)
    if(NOT "${result}" STREQUAL "${status}" OR NOT output MATCHES "${pattern}")
        message(FATAL_ERROR "${source} ${ARGN}: expected ${status}/${pattern}; got ${result}\n${output}\n${errors}")
    endif()
endfunction()
run_case(execution 0 "2 tests, 0 failed" expected nested)
run_case(execution 1 "8 tests, 5 failed")
run_case(cleanup 1 "5 tests, 4 failed")
execute_process(COMMAND "${HGL}" test "${SOURCE}/cleanup.hgl" combined_failure
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE errors)
if(NOT result EQUAL 1 OR NOT output MATCHES "negative duration" OR NOT output MATCHES "cleanup failed:.*cleanup sentinel")
    message(FATAL_ERROR "execution and teardown failures were not both reported:\n${output}\n${errors}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env "HGL_CXX=${OUT}/missing-compiler"
    "HGL_CACHE_DIR=${OUT}/missing-compiler-cache" "HGL_ARTIFACT_DIR=${OUT}"
    "${HGL}" test "${SOURCE}/execution.hgl" expected
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE errors)
if(NOT result EQUAL 1 OR NOT errors MATCHES "native compilation failed")
    message(FATAL_ERROR "build failure satisfied raises:\n${output}\n${errors}")
endif()
