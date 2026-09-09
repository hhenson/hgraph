if(NOT HGL OR NOT SOURCE OR NOT OUT)
    message(FATAL_ERROR "HGL, SOURCE and OUT are required")
endif()

set(_api "${SOURCE}/codegen/part-api.hgl")
set(_implementation "${SOURCE}/codegen/part-implementation.hgl")
set(_zeta "${SOURCE}/driver/module-parts/zeta.hgl")

function(run_hgl result output)
    execute_process(
        COMMAND "${HGL}" ${ARGN}
        RESULT_VARIABLE _result
        OUTPUT_VARIABLE _stdout
        ERROR_VARIABLE _stderr)
    set(${result} "${_result}" PARENT_SCOPE)
    set(${output} "${_stdout}\n${_stderr}" PARENT_SCOPE)
endfunction()

file(REMOVE_RECURSE "${OUT}")
file(MAKE_DIRECTORY "${OUT}/first" "${OUT}/second")

run_hgl(_check_result _check_output
    check "${_api}" --part "${_implementation}" --part "${_zeta}" --dump-hir)
if(NOT _check_result EQUAL 0 OR NOT _check_output MATCHES "HIR typed module checks.parts")
    message(FATAL_ERROR "checking a multi-file module failed:\n${_check_output}")
endif()

run_hgl(_test_result _test_output
    test "${_api}" --part "${_implementation}" --part "${_zeta}")
if(NOT _test_result EQUAL 0 OR NOT _test_output MATCHES "1 test, 0 failed")
    message(FATAL_ERROR "testing a multi-file module failed:\n${_test_output}")
endif()

run_hgl(_run_result _run_output
    run "${_api}" --part "${_implementation}" --part "${_zeta}"
    --entry configured --set value=7 --end 1us)
if(NOT _run_result EQUAL 0 OR NOT _run_output MATCHES "1970-01-01T00:00:00\\.000001Z 7")
    message(FATAL_ERROR "running a multi-file module failed:\n${_run_output}")
endif()

run_hgl(_first_result _first_output
    emit-cpp "${_api}" --part "${_implementation}" --part "${_zeta}" --out-dir "${OUT}/first")
run_hgl(_second_result _second_output
    emit-cpp "${_api}" --part "${_zeta}" --part "${_implementation}" --out-dir "${OUT}/second")
if(NOT _first_result EQUAL 0 OR NOT _second_result EQUAL 0)
    message(FATAL_ERROR "emitting a multi-file module failed:\n${_first_output}\n${_second_output}")
endif()

foreach(_artifact IN ITEMS part-api.h part-api.cpp part-api.hgl-module.json)
    file(READ "${OUT}/first/${_artifact}" _first)
    file(READ "${OUT}/second/${_artifact}" _second)
    if(NOT _first STREQUAL _second)
        message(FATAL_ERROR "module-part order changed generated ${_artifact}")
    endif()
endforeach()
file(READ "${OUT}/first/part-api.cpp" _generated_source)
if(NOT _generated_source MATCHES "part-implementation\\.hgl:[0-9]+")
    message(FATAL_ERROR "generated source did not retain its original part location:\n${_generated_source}")
endif()
if(NOT _first MATCHES "\"identity\"[ \t]*:[ \t]*\"checks.parts\"")
    message(FATAL_ERROR "assembled descriptor has the wrong identity:\n${_first}")
endif()

run_hgl(_mismatch_result _mismatch_output
    check "${_api}" --part "${SOURCE}/driver/module-parts/mismatched-module.hgl")
if(_mismatch_result EQUAL 0 OR
   NOT _mismatch_output MATCHES "mismatched-module\\.hgl:[0-9]+:[0-9]+" OR
   NOT _mismatch_output MATCHES "declares 'checks.other', expected 'checks.parts'")
    message(FATAL_ERROR "a mismatched module part was not diagnosed at its source:\n${_mismatch_output}")
endif()

run_hgl(_duplicate_part_result _duplicate_part_output
    check "${_api}" --part "${_implementation}" --part "${SOURCE}/driver/module-parts/duplicate-part.hgl")
if(_duplicate_part_result EQUAL 0 OR
   NOT _duplicate_part_output MATCHES "module part 'implementation' is declared twice" OR
   NOT _duplicate_part_output MATCHES "first part declaration is here")
    message(FATAL_ERROR "a duplicate part name was not diagnosed:\n${_duplicate_part_output}")
endif()

run_hgl(_missing_part_result _missing_part_output
    check "${_api}" --part "${SOURCE}/driver/module-parts/missing-part.hgl")
if(_missing_part_result EQUAL 0 OR
   NOT _missing_part_output MATCHES "every file in a multi-file module declares 'part <name>'")
    message(FATAL_ERROR "a missing part name was not diagnosed:\n${_missing_part_output}")
endif()

run_hgl(_duplicate_decl_result _duplicate_decl_output
    check "${_api}" --part "${_implementation}" --part "${SOURCE}/driver/module-parts/duplicate-declaration.hgl")
if(_duplicate_decl_result EQUAL 0 OR
   NOT _duplicate_decl_output MATCHES "duplicate-declaration\\.hgl:[0-9]+:[0-9]+" OR
   NOT _duplicate_decl_output MATCHES "private_forward")
    message(FATAL_ERROR "a cross-part duplicate declaration was not mapped to its source:\n${_duplicate_decl_output}")
endif()
