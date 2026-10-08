if(NOT HGL OR NOT SOURCE)
    message(FATAL_ERROR "HGL and SOURCE are required")
endif()

# The values alone cannot distinguish evaluation order. Capture the existing
# logger from both streams and check the calls made by the pinned spec example.
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env "HGL_DISABLE_CACHE=1"
        "${HGL}" test "${SOURCE}"
        ordinary_tuple_written_order ordinary_tuple_nested_written_order
    RESULT_VARIABLE _result
    OUTPUT_VARIABLE _trace
    ERROR_VARIABLE _trace)
string(REPLACE "\r\n" "\n" _trace "${_trace}")
if(NOT _result EQUAL 0 OR NOT _trace MATCHES "Executed: 2 tests, 0 failed")
    message(FATAL_ERROR "tuple order examples failed:\n${_trace}")
endif()
foreach(_name ordinary_tuple_written_order ordinary_tuple_nested_written_order)
    string(FIND "${_trace}" "${_name} ... ok [executed]" _executed)
    if(_executed EQUAL -1)
        message(FATAL_ERROR "tuple order example did not execute: ${_name}\n${_trace}")
    endif()
endforeach()
string(REGEX MATCHALL "\\[hgraph\\] \\[info\\] [0-9]+[^\n]*" _messages "${_trace}")
set(_labels)
foreach(_message IN LISTS _messages)
    string(REGEX REPLACE "^\\[hgraph\\] \\[info\\] " "" _label "${_message}")
    list(APPEND _labels "${_label}")
endforeach()
if(NOT "${_labels}" STREQUAL "2;1;3;2;1")
    message(FATAL_ERROR "unexpected tuple logger order: ${_labels}\n${_trace}")
endif()
message(STATUS "tuple logger order: 2,1,3,2,1; 2 tests passed")
