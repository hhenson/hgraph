# Run `hgl check` over an intentionally invalid design-corpus fixture
# (stdlib/README.md, "Fixture status") and pass only when the check fails and
# its output contains every expectation the fixture declares as a leading
# `// expect: <substring>` comment.
#   cmake -DHGL=<hgl> -DSOURCE=<fixture.hgl> -P check_invalid_fixture.cmake
if(NOT HGL OR NOT SOURCE)
    message(FATAL_ERROR "HGL and SOURCE are required")
endif()

file(STRINGS "${SOURCE}" _expect_lines REGEX "^// expect: ")
if(NOT _expect_lines)
    message(FATAL_ERROR "${SOURCE} declares no '// expect: <substring>' comment")
endif()

execute_process(
    COMMAND "${HGL}" check "${SOURCE}"
    RESULT_VARIABLE _result
    OUTPUT_VARIABLE _stdout
    ERROR_VARIABLE _stderr)
set(_output "${_stdout}\n${_stderr}")
if(_result EQUAL 0)
    message(FATAL_ERROR "hgl check unexpectedly accepted ${SOURCE}:\n${_output}")
endif()
foreach(_line IN LISTS _expect_lines)
    string(REGEX REPLACE "^// expect: " "" _expected "${_line}")
    string(FIND "${_output}" "${_expected}" _at)
    if(_at EQUAL -1)
        message(FATAL_ERROR "hgl check output for ${SOURCE} lacks '${_expected}':\n${_output}")
    endif()
endforeach()
