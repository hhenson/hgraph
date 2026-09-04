if(NOT HGL OR NOT SOURCE OR NOT OUT)
    message(FATAL_ERROR "HGL, SOURCE and OUT are required")
endif()

file(REMOVE_RECURSE "${OUT}")
file(MAKE_DIRECTORY "${OUT}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "HGL_CXX=${OUT}/missing-cxx"
        "HGL_ARTIFACT_DIR=${OUT}"
        "${HGL}" test "${SOURCE}"
    RESULT_VARIABLE _result
    OUTPUT_VARIABLE _stdout
    ERROR_VARIABLE _stderr)
set(_output "${_stdout}\n${_stderr}")
if(_result EQUAL 0)
    message(FATAL_ERROR "a missing native compiler unexpectedly succeeded:\n${_output}")
endif()
if(NOT _output MATCHES "native compilation failed")
    message(FATAL_ERROR "the compiler failure was not diagnosed:\n${_output}")
endif()
if(NOT _output MATCHES "artifacts retained in")
    message(FATAL_ERROR "the retained artifact directory was not reported:\n${_output}")
endif()
file(GLOB _artifacts "${OUT}/hgl-*")
if(NOT _artifacts)
    message(FATAL_ERROR "the failed native build retained no artifact directory")
endif()
file(REMOVE_RECURSE "${OUT}")
