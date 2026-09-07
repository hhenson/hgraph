if(NOT HGL OR NOT HELPER OR NOT TEMPLATE OR NOT SOURCE OR NOT OUT OR NOT PYTHON OR NOT NATIVE_DESCRIPTOR OR NOT GENERATOR)
    message(FATAL_ERROR "HGL, HELPER, TEMPLATE, SOURCE, OUT, PYTHON, NATIVE_DESCRIPTOR and GENERATOR are required")
endif()

file(REMOVE_RECURSE "${OUT}")
file(MAKE_DIRECTORY "${OUT}/sdk/lib/cmake/hgl" "${OUT}/bin")
file(COPY "${HELPER}" "${TEMPLATE}" DESTINATION "${OUT}/sdk/lib/cmake/hgl")
file(COPY "${HGL}" DESTINATION "${OUT}/bin")
get_filename_component(_hgl_name "${HGL}" NAME)
set(_installed_hgl "${OUT}/bin/${_hgl_name}")

execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${SOURCE}" -B "${OUT}/build" -G "${GENERATOR}"
        "-DHGL_LANGUAGE_CMAKE=${OUT}/sdk/lib/cmake/hgl/HglLanguage.cmake"
        "-DHGL_EXECUTABLE=${_installed_hgl}"
        "-DNATIVE_DESCRIPTOR=${NATIVE_DESCRIPTOR}"
    RESULT_VARIABLE _configure_result
    OUTPUT_VARIABLE _configure_out
    ERROR_VARIABLE _configure_err)
if(NOT _configure_result EQUAL 0)
    message(FATAL_ERROR "package fixture configure failed:\n${_configure_out}\n${_configure_err}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${OUT}/build" --target hgl_fixture_generate --config Debug
    RESULT_VARIABLE _first_result
    OUTPUT_VARIABLE _first_out
    ERROR_VARIABLE _first_err)
if(NOT _first_result EQUAL 0)
    message(FATAL_ERROR "first package generation failed:\n${_first_out}\n${_first_err}")
endif()
set(_descriptor "${OUT}/build/hgl/hgl_fixture/src/unit.hgl-module.json")
if(NOT EXISTS "${_descriptor}")
    message(FATAL_ERROR "package generation did not produce '${_descriptor}'")
endif()
file(READ "${_descriptor}" _descriptor_text)
if(NOT _descriptor_text MATCHES "\"format\"[ 	]*:[ 	]*\"hgl.module\"" OR
   NOT _descriptor_text MATCHES "\"identity\"[ 	]*:[ 	]*\"pkg.new.hgl_state\"" OR
   NOT _descriptor_text MATCHES "\"schema\"[ 	]*:")
    message(FATAL_ERROR "generated package descriptor has the wrong envelope:\n${_descriptor_text}")
endif()
set(_generated_header "${OUT}/build/hgl/hgl_fixture/include/unit.h")
file(READ "${_generated_header}" _generated_header_text)
if(NOT _generated_header_text MATCHES "checks::native_dependency::blend")
    message(FATAL_ERROR "installed helper did not pass the linked target descriptor:\n${_generated_header_text}")
endif()
# `new` is a C++ keyword and `hgl_state` a name the generated node code
# reserves; the header, the descriptor's registration symbol and the Python
# bootstrap must agree on the escaped namespace, and only the compiler spells it.
if(NOT _generated_header_text MATCHES "namespace pkg::new_::hgl_state_")
    message(FATAL_ERROR "reserved module segments were not escaped in the header:\n${_generated_header_text}")
endif()
set(_bootstrap "${OUT}/build/hgl/hgl_fixture/_fixture_module.cpp")
if(NOT EXISTS "${_bootstrap}")
    message(FATAL_ERROR "package generation did not produce the Python bootstrap '${_bootstrap}'")
endif()
file(READ "${_bootstrap}" _bootstrap_text)
if(NOT _bootstrap_text MATCHES "pkg::new_::hgl_state_::register_operators\\(\\)" OR
   NOT _bootstrap_text MATCHES "#include <unit.h>")
    message(FATAL_ERROR "the Python bootstrap does not call the generated registration:\n${_bootstrap_text}")
endif()
execute_process(
    COMMAND "${_installed_hgl}" emit-cpp "${SOURCE}/unit.hgl" --print-namespace
        --module-descriptor "${NATIVE_DESCRIPTOR}"
    RESULT_VARIABLE _namespace_result
    OUTPUT_VARIABLE _namespace_out
    ERROR_VARIABLE _namespace_err
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT _namespace_result EQUAL 0 OR NOT _namespace_out STREQUAL "pkg::new_::hgl_state_")
    message(FATAL_ERROR "emit-cpp --print-namespace printed '${_namespace_out}':\n${_namespace_err}")
endif()
execute_process(
    COMMAND "${PYTHON}" -m json.tool "${_descriptor}"
    RESULT_VARIABLE _descriptor_json_result
    OUTPUT_QUIET
    ERROR_VARIABLE _descriptor_json_error)
if(NOT _descriptor_json_result EQUAL 0)
    message(FATAL_ERROR "generated package descriptor is not valid JSON:\n${_descriptor_json_error}")
endif()
execute_process(
    COMMAND "${PYTHON}" -m py_compile
        "${OUT}/build/python/_fixture/unit.py"
        "${OUT}/build/python/_fixture/__init__.py"
    RESULT_VARIABLE _python_result
    OUTPUT_VARIABLE _python_out
    ERROR_VARIABLE _python_err)
if(NOT _python_result EQUAL 0)
    message(FATAL_ERROR "generated Python wrapper is invalid:\n${_python_out}\n${_python_err}")
endif()

# The installed executable, not a target, must invalidate the custom command.
execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 1)
file(TOUCH_NOCREATE "${_installed_hgl}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${OUT}/build" --target hgl_fixture_generate --config Debug --verbose
    RESULT_VARIABLE _second_result
    OUTPUT_VARIABLE _second_out
    ERROR_VARIABLE _second_err)
if(NOT _second_result EQUAL 0)
    message(FATAL_ERROR "package regeneration failed:\n${_second_out}\n${_second_err}")
endif()
set(_second_log "${_second_out}\n${_second_err}")
if(NOT _second_log MATCHES "emit-cpp[^\n]*unit\\.hgl")
    message(FATAL_ERROR "touching installed hgl did not regenerate the module:\n${_second_log}")
endif()
