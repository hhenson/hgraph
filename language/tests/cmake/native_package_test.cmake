if(NOT BUILD OR NOT SOURCE OR NOT OUT OR NOT GENERATOR OR NOT HGL)
    message(FATAL_ERROR "BUILD, SOURCE, OUT, GENERATOR and HGL are required")
endif()

file(REMOVE_RECURSE "${OUT}")

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${BUILD}" --prefix "${OUT}/sdk" --component Development
    RESULT_VARIABLE _install_result
    OUTPUT_VARIABLE _install_out
    ERROR_VARIABLE _install_err)
if(NOT _install_result EQUAL 0)
    message(FATAL_ERROR "native-package SDK install failed:\n${_install_out}\n${_install_err}")
endif()

file(GLOB_RECURSE _targets "${OUT}/sdk/*/cmake/hgl/HglLanguageTargets.cmake")
list(LENGTH _targets _target_count)
if(NOT _target_count EQUAL 1)
    message(FATAL_ERROR "expected one installed HglLanguageTargets.cmake, found: ${_targets}")
endif()
list(GET _targets 0 _target_file)

execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${SOURCE}" -B "${OUT}/build" -G "${GENERATOR}"
        "-DHGL_LANGUAGE_TARGETS=${_target_file}"
    RESULT_VARIABLE _configure_result
    OUTPUT_VARIABLE _configure_out
    ERROR_VARIABLE _configure_err)
if(NOT _configure_result EQUAL 0)
    message(FATAL_ERROR "native-package consumer configure failed:\n${_configure_out}\n${_configure_err}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${OUT}/build" --config Release
    RESULT_VARIABLE _build_result
    OUTPUT_VARIABLE _build_out
    ERROR_VARIABLE _build_err)
if(NOT _build_result EQUAL 0)
    message(FATAL_ERROR "native-package consumer build failed:\n${_build_out}\n${_build_err}")
endif()

set(_descriptor "${OUT}/consumer.hgl-module.json")
if(WIN32)
    set(ENV{PATH} "${OUT}/sdk/bin;$ENV{PATH}")
endif()
execute_process(
    COMMAND "${OUT}/build/bin/hgl_native_package_consumer${CMAKE_EXECUTABLE_SUFFIX}" "${_descriptor}"
    RESULT_VARIABLE _run_result
    OUTPUT_VARIABLE _run_out
    ERROR_VARIABLE _run_err)
if(NOT _run_result EQUAL 0)
    message(FATAL_ERROR "native-package consumer failed:\n${_run_out}\n${_run_err}")
endif()

execute_process(
    COMMAND "${HGL}" check "${_descriptor}"
    RESULT_VARIABLE _check_result
    OUTPUT_VARIABLE _check_out
    ERROR_VARIABLE _check_err)
if(NOT _check_result EQUAL 0)
    message(FATAL_ERROR "installed consumer descriptor failed validation:\n${_check_out}\n${_check_err}")
endif()
