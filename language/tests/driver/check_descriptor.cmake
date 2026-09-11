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

set(descriptor "${OUT}/midpoint.hgl-module.json")
execute_process(
    COMMAND "${HGL}" check "${descriptor}"
    RESULT_VARIABLE check_result
    OUTPUT_VARIABLE check_output
    ERROR_VARIABLE check_error
)
if(NOT check_result EQUAL 0)
    message(FATAL_ERROR "descriptor check failed (${check_result})\n${check_output}${check_error}")
endif()

execute_process(
    COMMAND "${HGL}" check "${descriptor}" --dump-hir
    RESULT_VARIABLE dump_result
    OUTPUT_VARIABLE dump_output
    ERROR_VARIABLE dump_error
)
if(NOT dump_result EQUAL 2 OR NOT dump_error MATCHES "descriptor check does not support syntax or IR dump options")
    message(FATAL_ERROR "descriptor dump options did not fail as usage (${dump_result})\n${dump_output}${dump_error}")
endif()

file(READ "${descriptor}" invalid_json)
string(REPLACE [["format_version": 3]] [["format_version": 99]] invalid_json "${invalid_json}")
set(invalid_descriptor "${OUT}/invalid.hgl-module.json")
file(WRITE "${invalid_descriptor}" "${invalid_json}")
execute_process(
    COMMAND "${HGL}" check "${invalid_descriptor}"
    RESULT_VARIABLE invalid_result
    OUTPUT_VARIABLE invalid_output
    ERROR_VARIABLE invalid_error
)
if(NOT invalid_result EQUAL 1 OR NOT invalid_error MATCHES [[\$.format_version: descriptor: unsupported descriptor format version 99]])
    message(FATAL_ERROR "invalid descriptor diagnostic mismatch (${invalid_result})\n${invalid_output}${invalid_error}")
endif()
