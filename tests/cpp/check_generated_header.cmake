# Measure the public include graph without a precompiled header. Standard-library
# size varies by toolchain, so use the absolute budget on the reference family.
cmake_minimum_required(VERSION 3.25)
file(MAKE_DIRECTORY "${WORK}")
set(probe "${WORK}/generated_header_probe.cpp")
file(WRITE "${probe}" [=[
#include <hgraph/types/generated.h>
#if defined(__GNUC__) && __GNUC__ == 15 && !defined(__clang__) && defined(__GLIBCXX__)
HGRAPH_GNU15_LIBSTDCXX
#endif
]=])
if(MSVC_FRONTEND)
    set(flags /nologo /std:c++latest /Zc:__cplusplus /EHsc /E /TP)
    set(define_prefix "/D")
    set(include_prefix "/I")
else()
    set(flags -std=c++23 -E -x c++)
    set(define_prefix "-D")
    set(include_prefix "-I")
endif()
foreach(dir IN LISTS INCLUDES)
    list(APPEND flags "${include_prefix}${dir}")
endforeach()
foreach(def IN LISTS DEFINITIONS)
    list(APPEND flags "${define_prefix}${def}")
endforeach()
execute_process(COMMAND "${COMPILER}" ${flags} "${probe}"
                OUTPUT_FILE "${WORK}/generated_header.i" COMMAND_ERROR_IS_FATAL ANY)
file(READ "${WORK}/generated_header.i" output)
string(REPLACE "\\" "/" output "${output}")
if(output MATCHES "/hgraph/(types/(type_resolution|call_args|frame|series|node_delta_builders|wiring_observer|graph_wiring|subgraph_wiring|metadata/type_realization|time_series/ts_delta)|runtime/shared_output_node)\\.h\"")
    message(FATAL_ERROR "Generated modules pulled in the full authoring header ${CMAKE_MATCH_0}")
endif()
string(REGEX MATCHALL "\n" newlines "${output}")
list(LENGTH newlines lines)
message(STATUS "Generated header: ${lines} preprocessed lines")
if(lines GREATER_EQUAL 170000 AND output MATCHES "HGRAPH_GNU15_LIBSTDCXX")
    message(FATAL_ERROR "Generated header exceeds the GCC 15 / libstdc++ budget of 169999 lines")
endif()
