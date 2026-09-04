if(NOT HGL OR NOT HELPER OR NOT TEMPLATE OR NOT SOURCE OR NOT OUT OR NOT PYTHON OR NOT GENERATOR)
    message(FATAL_ERROR "HGL, HELPER, TEMPLATE, SOURCE, OUT, PYTHON and GENERATOR are required")
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
