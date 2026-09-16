if(NOT BUILD OR NOT SOURCE OR NOT OUT OR NOT GENERATOR OR NOT HGL OR NOT CXX)
    message(FATAL_ERROR "BUILD, SOURCE, OUT, GENERATOR, HGL and CXX are required")
endif()

file(REMOVE_RECURSE "${OUT}")
file(REMOVE_RECURSE "${OUT}-relocated")

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

if(UNIX)
    set(_native_package "${OUT}/sdk/${INSTALL_LIBDIR}/${NATIVE_PACKAGE_NAME}")
    # Inspect the installed image, not just the CMake property: automatic link
    # paths are appended at install time and do not appear in INSTALL_RPATH.
    set(_expected_rpath ${PACKAGER_RPATH})
    if(APPLE)
        set(_inspect_tool "${OTOOL}")
    else()
        set(_inspect_tool "${OBJDUMP}")
    endif()
    if(NOT _inspect_tool OR NOT EXISTS "${_inspect_tool}")
        message(FATAL_ERROR
            "cannot inspect installed native-package runtime paths: no image inspector "
            "('${_inspect_tool}'). otool is required on macOS, objdump elsewhere.")
    endif()
    if(APPLE)
        list(APPEND _expected_rpath "@loader_path")
        execute_process(COMMAND "${_inspect_tool}" -l "${_native_package}"
            RESULT_VARIABLE _inspect_result OUTPUT_VARIABLE _image ERROR_VARIABLE _inspect_err)
        string(REGEX MATCHALL "cmd LC_RPATH\n[^\n]*\n[^\n]*" _commands "${_image}")
        set(_actual_rpath "")
        foreach(_command IN LISTS _commands)
            string(REGEX REPLACE ".*\n[ \t]*path (.*) \\(offset [0-9]+\\)" "\\1" _path "${_command}")
            list(APPEND _actual_rpath "${_path}")
        endforeach()
    else()
        list(APPEND _expected_rpath "$ORIGIN")
        execute_process(COMMAND "${_inspect_tool}" -p "${_native_package}"
            RESULT_VARIABLE _inspect_result OUTPUT_VARIABLE _image ERROR_VARIABLE _inspect_err)
        string(REGEX MATCH "(RUNPATH|RPATH)[ \t]+([^\n]+)" _rpath_line "${_image}")
        string(REPLACE ":" ";" _actual_rpath "${CMAKE_MATCH_2}")
    endif()
    list(REMOVE_DUPLICATES _expected_rpath)
    if(NOT _inspect_result EQUAL 0)
        message(FATAL_ERROR
            "cannot inspect installed native-package runtime paths with '${_inspect_tool}' "
            "(exit ${_inspect_result}): ${_inspect_err}")
    endif()
    if(NOT "${_actual_rpath}" STREQUAL "${_expected_rpath}")
        message(FATAL_ERROR "installed native-package runtime paths '${_actual_rpath}' differ from '${_expected_rpath}'")
    endif()

    if(SIMDJSON_LIBRARY)
        # Copy the real image under every versioned alias. TARGET_FILE may
        # already be the fully versioned file, with no symlink chain to follow.
        get_filename_component(_simdjson_dir "${SIMDJSON_LIBRARY}" DIRECTORY)
        if(APPLE)
            file(GLOB _simdjson_images "${_simdjson_dir}/libsimdjson*.dylib")
        else()
            file(GLOB _simdjson_images "${_simdjson_dir}/libsimdjson.so*")
        endif()
        if(NOT _simdjson_images)
            message(FATAL_ERROR "no shared simdjson images found beside '${SIMDJSON_LIBRARY}'")
        endif()
        foreach(_image IN LISTS _simdjson_images)
            get_filename_component(_name "${_image}" NAME)
            configure_file("${_image}" "${OUT}/sdk/${INSTALL_LIBDIR}/${_name}" COPYONLY)
        endforeach()
    endif()
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
        "-DSDK_LIBDIR=${INSTALL_LIBDIR}"
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

if(UNIX)
    # The original SDK path disappears. Neither the executable nor the facade
    # can rely on an absolute path to the original installation.
    file(RENAME "${OUT}" "${OUT}-relocated")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env --unset=LD_LIBRARY_PATH --unset=DYLD_LIBRARY_PATH
            --unset=DYLD_FALLBACK_LIBRARY_PATH
            "${OUT}-relocated/build/bin/hgl_native_package_relocation_consumer"
        RESULT_VARIABLE _relocation_result OUTPUT_VARIABLE _relocation_out ERROR_VARIABLE _relocation_err)
    if(NOT _relocation_result EQUAL 0)
        message(FATAL_ERROR "relocated native-package consumer failed:\n${_relocation_out}\n${_relocation_err}")
    endif()
    if(SIMDJSON_LIBRARY)
        file(GET_RUNTIME_DEPENDENCIES
            LIBRARIES "${OUT}-relocated/sdk/${INSTALL_LIBDIR}/${NATIVE_PACKAGE_NAME}"
            RESOLVED_DEPENDENCIES_VAR _resolved
            UNRESOLVED_DEPENDENCIES_VAR _unresolved)
        set(_found_simdjson FALSE)
        foreach(_dependency IN LISTS _resolved)
            get_filename_component(_name "${_dependency}" NAME)
            if(_name MATCHES "^libsimdjson")
                file(REAL_PATH "${_dependency}" _resolved_simdjson)
                file(REAL_PATH "${OUT}-relocated/sdk/${INSTALL_LIBDIR}/${_name}" _relocated_simdjson)
                if(NOT _resolved_simdjson STREQUAL _relocated_simdjson)
                    message(FATAL_ERROR "simdjson resolved outside the relocated SDK: ${_dependency}")
                endif()
                set(_found_simdjson TRUE)
            endif()
        endforeach()
        if(NOT _found_simdjson OR _unresolved)
            message(FATAL_ERROR "relocated dependency resolution failed: simdjson=${_found_simdjson}, unresolved=${_unresolved}")
        endif()
    endif()
endif()
