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
    check "${_api}" --part "${_implementation}" --part "${_zeta}" --part "${SOURCE}/driver/module-parts/missing-part.hgl")
if(NOT _missing_part_result EQUAL 0)
    message(FATAL_ERROR "an unnamed interface with a named part was rejected:\n${_missing_part_output}")
endif()

run_hgl(_duplicate_decl_result _duplicate_decl_output
    check "${_api}" --part "${_implementation}" --part "${SOURCE}/driver/module-parts/duplicate-declaration.hgl")
if(_duplicate_decl_result EQUAL 0 OR
   NOT _duplicate_decl_output MATCHES "duplicate-declaration\\.hgl:[0-9]+:[0-9]+" OR
   NOT _duplicate_decl_output MATCHES "private_forward")
    message(FATAL_ERROR "a cross-part duplicate declaration was not mapped to its source:\n${_duplicate_decl_output}")
endif()

# Native requirements complete a shared declaration, never another overload.
set(_native_api "${SOURCE}/codegen/native-provider.hgl")
set(_native_impl "${SOURCE}/codegen/native-provider-impl.hgl")
run_hgl(_native_result _native_output check "${_native_api}" --part "${_native_impl}" --dump-hir)
if(NOT _native_result EQUAL 0 OR NOT _native_output MATCHES "implementation=value inject=logger")
    message(FATAL_ERROR "native implementation requirements were not selected:\n${_native_output}")
endif()
run_hgl(_native_result _native_output check "${_native_impl}" --part "${_native_api}" --dump-hir)
if(NOT _native_result EQUAL 0 OR NOT _native_output MATCHES "implementation=value inject=logger")
    message(FATAL_ERROR "reversing native part order changed selection:\n${_native_output}")
endif()
run_hgl(_native_result _native_output check "${_native_api}" --part "${_native_impl}"
    --part "${SOURCE}/codegen/native-provider-rust-impl.hgl")
if(_native_result EQUAL 0 OR NOT _native_output MATCHES "more than one selected implementation")
    message(FATAL_ERROR "selecting two native target implementations was accepted:\n${_native_output}")
endif()
file(WRITE "${OUT}/contract.hgl" "module checks.shapes\nnative fn filter(value: i64, const limit: i64) -> i64\n")
foreach(_shape IN ITEMS graph node)
    if(_shape STREQUAL "graph")
        set(_body "{}")
    else()
        set(_body "{ inject out, logger\n start; when; stop; }")
    endif()
    file(WRITE "${OUT}/impl.hgl" "module checks.shapes part cpp_impl\nnative fn filter(value: i64, const limit: i64) -> i64 ${_body}\n")
    run_hgl(_shape_result _shape_output check "${OUT}/contract.hgl" --part "${OUT}/impl.hgl" --dump-hgraph-ir)
    if(NOT _shape_result EQUAL 0 OR NOT _shape_output MATCHES "implementation=${_shape}")
        message(FATAL_ERROR "native ${_shape} shape was not retained:\n${_shape_output}")
    endif()
endforeach()

# Target requirements alter the generated service ABI, never the public signature.
file(WRITE "${OUT}/value.hgl" "module checks.targets\nnative const fn f(value: i64) -> i64\n")
file(WRITE "${OUT}/plain.hgl" "module checks.targets part plain\nnative const fn f(value: i64) -> i64 {}\n")
file(WRITE "${OUT}/logging.hgl" "module checks.targets part logging\nnative const fn f(value: i64) -> i64 { inject logger }\n")
foreach(_target IN ITEMS plain logging)
    run_hgl(_result _output emit-native-rust "${OUT}/value.hgl" --part "${OUT}/${_target}.hgl" --out "${OUT}/${_target}.rs")
    if(NOT _result EQUAL 0)
        message(FATAL_ERROR "selected native requirements failed emission:\n${_output}")
    endif()
endforeach()
file(READ "${OUT}/plain.rs" _plain)
file(READ "${OUT}/logging.rs" _logging)
if(_plain MATCHES "dyn Logger" OR NOT _logging MATCHES "dyn Logger")
    message(FATAL_ERROR "target-specific capability requests were not reflected in the Rust binding")
endif()
run_hgl(_result _output emit-native-rust "${OUT}/value.hgl" --out "${OUT}/missing.rs")
if(_result EQUAL 0 OR NOT _output MATCHES "requires a selected implementation part")
    message(FATAL_ERROR "a bare declaration silently acquired an implementation:\n${_output}")
endif()
