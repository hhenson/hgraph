if(NOT BUILD OR NOT SOURCE OR NOT OUT OR NOT GENERATOR OR NOT HGL OR NOT CXX)
    message(FATAL_ERROR "BUILD, SOURCE, OUT, GENERATOR, HGL and CXX are required")
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

# hgl::core_native is a real generated library and therefore links the hgraph
# SDK. Install the ordinary (unclassified) hgraph targets beside the HGL
# development component before configuring the isolated consumer.
execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${BUILD}" --prefix "${OUT}/sdk" --component Unspecified
    RESULT_VARIABLE _runtime_install_result
    OUTPUT_VARIABLE _runtime_install_out
    ERROR_VARIABLE _runtime_install_err)
if(NOT _runtime_install_result EQUAL 0)
    message(FATAL_ERROR "hgraph SDK install failed:\n${_runtime_install_out}\n${_runtime_install_err}")
endif()

file(GLOB_RECURSE _targets "${OUT}/sdk/*/cmake/hgl/HglLanguageTargets.cmake")
list(LENGTH _targets _target_count)
if(NOT _target_count EQUAL 1)
    message(FATAL_ERROR "expected one installed HglLanguageTargets.cmake, found: ${_targets}")
endif()
list(GET _targets 0 _target_file)
get_filename_component(_hgl_cmake_dir "${_target_file}" DIRECTORY)
set(_hgl_language_cmake "${_hgl_cmake_dir}/HglLanguage.cmake")
if(NOT EXISTS "${_hgl_language_cmake}")
    message(FATAL_ERROR "installed HglLanguage.cmake was not found beside '${_target_file}'")
endif()

# Discover the installed data location rather than assuming GNUInstallDirs'
# default share/ spelling.
file(GLOB_RECURSE _native_anchors "${OUT}/sdk/*/hgl/stdlib/hgraph/native.hgl")
list(LENGTH _native_anchors _native_anchor_count)
if(NOT _native_anchor_count EQUAL 1)
    message(FATAL_ERROR "expected one installed native.hgl anchor, found: ${_native_anchors}")
endif()
list(GET _native_anchors 0 _native_anchor)
get_filename_component(_native_source_dir "${_native_anchor}" DIRECTORY)

execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${SOURCE}" -B "${OUT}/build" -G "${GENERATOR}"
        "-DCMAKE_CXX_COMPILER=${CXX}"
        -DCMAKE_BUILD_TYPE=Release
        "-DCMAKE_PREFIX_PATH=${OUT}/sdk;${DEPENDENCY_PREFIX_PATH}"
        "-DHGL_LANGUAGE_CMAKE=${_hgl_language_cmake}"
        "-DHGL_EXECUTABLE=${HGL}"
        "-DHGL_STDLIB_SOURCE=${_native_source_dir}"
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

# Compile the installed HGL source parts as well as consuming the prebuilt
# library. Missing source installation must not be masked by native.h/.a.
execute_process(
    COMMAND "${OUT}/build/bin/hgl_native_parts_consumer${CMAKE_EXECUTABLE_SUFFIX}"
    RESULT_VARIABLE _parts_result
    OUTPUT_VARIABLE _parts_out
    ERROR_VARIABLE _parts_err)
if(NOT _parts_result EQUAL 0)
    message(FATAL_ERROR "installed native parts consumer failed:\n${_parts_out}\n${_parts_err}")
endif()

execute_process(
    COMMAND "${OUT}/build/bin/hgl_imported_operator_consumer${CMAKE_EXECUTABLE_SUFFIX}"
    RESULT_VARIABLE _imported_result
    OUTPUT_VARIABLE _imported_out
    ERROR_VARIABLE _imported_err)
if(NOT _imported_result EQUAL 0)
    message(FATAL_ERROR "installed imported operator consumer failed:\n${_imported_out}\n${_imported_err}")
endif()

execute_process(
    COMMAND "${HGL}" check "${_descriptor}"
    RESULT_VARIABLE _check_result
    OUTPUT_VARIABLE _check_out
    ERROR_VARIABLE _check_err)
if(NOT _check_result EQUAL 0)
    message(FATAL_ERROR "installed consumer descriptor failed validation:\n${_check_out}\n${_check_err}")
endif()
