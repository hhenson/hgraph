# Emits a module that exports recursive structs and checks its descriptor
# without loading code (ADR 0012, descriptor format 6): every edge is marked,
# and a layout whose edge is not optional is rejected.
if(NOT DEFINED HGL OR NOT DEFINED SOURCE OR NOT DEFINED OUT)
    message(FATAL_ERROR "HGL, SOURCE, and OUT are required")
endif()

file(MAKE_DIRECTORY "${OUT}")
execute_process(
    COMMAND "${HGL}" emit-cpp "${SOURCE}" --out-dir "${OUT}"
    RESULT_VARIABLE emit_result
    OUTPUT_VARIABLE emit_output
    ERROR_VARIABLE emit_error
)
if(NOT emit_result EQUAL 0)
    message(FATAL_ERROR "emit-cpp failed (${emit_result})\n${emit_output}${emit_error}")
endif()

get_filename_component(stem "${SOURCE}" NAME_WE)
set(descriptor "${OUT}/${stem}.hgl-module.json")
execute_process(
    COMMAND "${HGL}" check "${descriptor}"
    RESULT_VARIABLE check_result
    OUTPUT_VARIABLE check_output
    ERROR_VARIABLE check_error
)
if(NOT check_result EQUAL 0)
    message(FATAL_ERROR "descriptor check failed (${check_result})\n${check_output}${check_error}")
endif()

# Node.next, Tree.left, Tree.right, Add.lhs and Add.rhs.
file(READ "${descriptor}" json)
string(REGEX MATCHALL "\"recursive\": true" edges "${json}")
list(LENGTH edges edge_count)
if(NOT edge_count EQUAL 5)
    message(FATAL_ERROR "expected 5 recursive edges in ${descriptor}, found ${edge_count}")
endif()

# An empty fingerprint is not checked, so the edited layout reaches validation.
string(REGEX REPLACE "\"descriptor_fingerprint\": \"[^\"]*\"" "\"descriptor_fingerprint\": \"\"" json "${json}")
string(REGEX REPLACE "\"optional\": true(,[^}]*\"recursive\": true)" "\"optional\": false\\1" json "${json}")
set(required_edge "${OUT}/required-edge.hgl-module.json")
file(WRITE "${required_edge}" "${json}")
execute_process(
    COMMAND "${HGL}" check "${required_edge}"
    RESULT_VARIABLE required_result
    OUTPUT_VARIABLE required_output
    ERROR_VARIABLE required_error
)
if(NOT required_result EQUAL 1 OR NOT required_error MATCHES "a recursive edge must be optional")
    message(FATAL_ERROR "a required edge was not rejected (${required_result})\n${required_output}${required_error}")
endif()
