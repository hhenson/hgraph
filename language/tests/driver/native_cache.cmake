if(NOT HGL OR NOT CXX OR NOT SOURCE OR NOT OUT)
    message(FATAL_ERROR "HGL, CXX, SOURCE and OUT are required")
endif()

set(_cache "${OUT}/cache")
set(_artifacts "${OUT}/artifacts")
file(REMOVE_RECURSE "${OUT}")
file(MAKE_DIRECTORY "${_artifacts}")

function(run_cached source output_var result_var)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
            "HGL_CACHE_DIR=${_cache}"
            "HGL_ARTIFACT_DIR=${_artifacts}"
            "HGL_CACHE_TRACE=1"
            "${HGL}" test "${source}"
        RESULT_VARIABLE _result
        OUTPUT_VARIABLE _stdout
        ERROR_VARIABLE _stderr)
    set(${output_var} "${_stdout}\n${_stderr}" PARENT_SCOPE)
    set(${result_var} "${_result}" PARENT_SCOPE)
endfunction()

function(require_key output disposition key_var)
    string(REGEX MATCH "hgl native cache ${disposition} ([0-9a-f]+)" _match "${output}")
    if(NOT _match)
        message(FATAL_ERROR "expected a cache ${disposition}:\n${output}")
    endif()
    set(_key "${CMAKE_MATCH_1}")
    string(LENGTH "${_key}" _length)
    if(NOT _length EQUAL 64)
        message(FATAL_ERROR "cache key is not a SHA-256 digest: ${_key}")
    endif()
    set(${key_var} "${_key}" PARENT_SCOPE)
endfunction()

run_cached("${SOURCE}" _cold_output _cold_result)
if(NOT _cold_result EQUAL 0)
    message(FATAL_ERROR "cold cached run failed:\n${_cold_output}")
endif()
require_key("${_cold_output}" "miss" _original_key)
set(_entry "${_cache}/v2/${_original_key}")
if(NOT EXISTS "${_entry}/complete" OR NOT EXISTS "${_entry}/manifest.txt")
    message(FATAL_ERROR "cold run did not publish a complete cache entry")
endif()
if(APPLE)
    set(_image "${_entry}/module.bundle")
else()
    set(_image "${_entry}/module.so")
endif()
if(NOT EXISTS "${_image}")
    message(FATAL_ERROR "cold run did not publish its native image")
endif()

run_cached("${SOURCE}" _warm_output _warm_result)
if(NOT _warm_result EQUAL 0)
    message(FATAL_ERROR "warm cached run failed:\n${_warm_output}")
endif()
require_key("${_warm_output}" "hit" _warm_key)
if(NOT "${_warm_key}" STREQUAL "${_original_key}")
    message(FATAL_ERROR "warm run selected a different cache key")
endif()

# An image whose bytes no longer match its completion marker is never loaded.
# The next command rebuilds the same key and quarantines the damaged entry.
file(APPEND "${_image}" "corrupt")
run_cached("${SOURCE}" _repair_output _repair_result)
if(NOT _repair_result EQUAL 0)
    message(FATAL_ERROR "cache repair run failed:\n${_repair_output}")
endif()
require_key("${_repair_output}" "miss" _repair_key)
if(NOT "${_repair_key}" STREQUAL "${_original_key}")
    message(FATAL_ERROR "repair run did not rebuild the original key")
endif()
file(GLOB _quarantined LIST_DIRECTORIES true "${_cache}/v2/.incomplete-*")
if(NOT _quarantined)
    message(FATAL_ERROR "the damaged cache entry was not quarantined")
endif()

run_cached("${SOURCE}" _repaired_output _repaired_result)
if(NOT _repaired_result EQUAL 0)
    message(FATAL_ERROR "repaired cache did not remain reusable:\n${_repaired_output}")
endif()
require_key("${_repaired_output}" "hit" _repaired_key)

# An unidentified compiler disables reuse. A failed build cannot publish over
# the valid entry.
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "HGL_CACHE_DIR=${_cache}"
        "HGL_ARTIFACT_DIR=${_artifacts}"
        "HGL_CACHE_TRACE=1"
        "HGL_CXX=${OUT}/missing-cxx"
        "${HGL}" test "${SOURCE}"
    RESULT_VARIABLE _compiler_result
    OUTPUT_VARIABLE _compiler_stdout
    ERROR_VARIABLE _compiler_stderr)
set(_compiler_output "${_compiler_stdout}\n${_compiler_stderr}")
if(_compiler_result EQUAL 0)
    message(FATAL_ERROR "a missing compiler unexpectedly reused a cache entry")
endif()
if(NOT _compiler_output MATCHES "hgl native cache unavailable: compiler executable cannot be identified")
    message(FATAL_ERROR "an unidentified compiler did not disable caching:\n${_compiler_output}")
endif()
if(NOT EXISTS "${_entry}/complete")
    message(FATAL_ERROR "a failed compiler invocation damaged the valid cache entry")
endif()

# The compiler's bytes are part of its identity even when its path, reported
# version, target and behavior stay the same.
set(_wrapper "${OUT}/compiler-wrapper")
set(_wrapper_cache "${OUT}/wrapper-cache")
function(write_compiler_wrapper marker)
    file(WRITE "${_wrapper}" "#!/bin/sh\n# ${marker}\nexec \"${CXX}\" \"$@\"\n")
    file(CHMOD "${_wrapper}"
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
endfunction()
function(run_with_wrapper output_var result_var)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
            "HGL_CACHE_DIR=${_wrapper_cache}"
            "HGL_ARTIFACT_DIR=${_artifacts}"
            "HGL_CACHE_TRACE=1"
            "HGL_CXX=${_wrapper}"
            "${HGL}" test "${SOURCE}"
        RESULT_VARIABLE _result
        OUTPUT_VARIABLE _stdout
        ERROR_VARIABLE _stderr)
    set(${output_var} "${_stdout}\n${_stderr}" PARENT_SCOPE)
    set(${result_var} "${_result}" PARENT_SCOPE)
endfunction()

write_compiler_wrapper("first identity")
run_with_wrapper(_wrapper_first_output _wrapper_first_result)
if(NOT _wrapper_first_result EQUAL 0)
    message(FATAL_ERROR "first compiler-wrapper run failed:\n${_wrapper_first_output}")
endif()
require_key("${_wrapper_first_output}" "miss" _wrapper_first_key)

write_compiler_wrapper("changed identity")
run_with_wrapper(_wrapper_second_output _wrapper_second_result)
if(NOT _wrapper_second_result EQUAL 0)
    message(FATAL_ERROR "changed compiler-wrapper run failed:\n${_wrapper_second_output}")
endif()
require_key("${_wrapper_second_output}" "miss" _wrapper_second_key)
if("${_wrapper_second_key}" STREQUAL "${_wrapper_first_key}")
    message(FATAL_ERROR "changed compiler bytes reused the original cache key")
endif()

# A checked source change must select a different digest and preserve the old
# entry. This default is not used by the fixture's tests, so behavior remains
# valid while the emitted native source changes.
file(READ "${SOURCE}" _source_text)
string(REPLACE "const value: f64 = 2.0" "const value: f64 = 2.5" _changed_text "${_source_text}")
if("${_changed_text}" STREQUAL "${_source_text}")
    message(FATAL_ERROR "the cache fixture no longer contains its replacement marker")
endif()
set(_changed_source "${OUT}/runtime-changed.hgl")
file(WRITE "${_changed_source}" "${_changed_text}")
run_cached("${_changed_source}" _changed_output _changed_result)
if(NOT _changed_result EQUAL 0)
    message(FATAL_ERROR "source-invalidation run failed:\n${_changed_output}")
endif()
require_key("${_changed_output}" "miss" _changed_key)
if("${_changed_key}" STREQUAL "${_original_key}")
    message(FATAL_ERROR "changed source reused the original cache key")
endif()
if(NOT EXISTS "${_entry}/complete" OR NOT EXISTS "${_cache}/v2/${_changed_key}/complete")
    message(FATAL_ERROR "source invalidation did not preserve both complete entries")
endif()

# Cache storage is an optimization. An unusable cache root is reported under
# tracing, but the command still executes from a transient image.
set(_unavailable_cache "${OUT}/not-a-directory")
file(WRITE "${_unavailable_cache}" "cache root obstruction")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "HGL_CACHE_DIR=${_unavailable_cache}"
        "HGL_ARTIFACT_DIR=${_artifacts}"
        "HGL_CACHE_TRACE=1"
        "${HGL}" test "${SOURCE}"
    RESULT_VARIABLE _fallback_result
    OUTPUT_VARIABLE _fallback_stdout
    ERROR_VARIABLE _fallback_stderr)
set(_fallback_output "${_fallback_stdout}\n${_fallback_stderr}")
if(NOT _fallback_result EQUAL 0 OR NOT _fallback_output MATCHES "hgl native cache unavailable")
    message(FATAL_ERROR "an unavailable cache did not fall back to a transient image:\n${_fallback_output}")
endif()

# Without an explicit or per-user cache root, never fall back to a predictable
# shared directory under the system temporary root.
set(_fallback_tmp "${OUT}/fallback-tmp")
file(MAKE_DIRECTORY "${_fallback_tmp}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        --unset=HGL_CACHE_DIR
        --unset=XDG_CACHE_HOME
        --unset=HOME
        "TMPDIR=${_fallback_tmp}"
        "HGL_ARTIFACT_DIR=${_artifacts}"
        "HGL_CACHE_TRACE=1"
        "${HGL}" test "${SOURCE}"
    RESULT_VARIABLE _no_user_cache_result
    OUTPUT_VARIABLE _no_user_cache_stdout
    ERROR_VARIABLE _no_user_cache_stderr)
set(_no_user_cache_output "${_no_user_cache_stdout}\n${_no_user_cache_stderr}")
if(NOT _no_user_cache_result EQUAL 0 OR
   NOT _no_user_cache_output MATCHES "hgl native cache unavailable: no per-user cache directory")
    message(FATAL_ERROR "missing per-user cache variables did not select a transient image:\n${_no_user_cache_output}")
endif()
if(EXISTS "${_fallback_tmp}/hgl-cache")
    message(FATAL_ERROR "missing per-user cache variables created a shared-style temporary cache")
endif()

# Publication uses a complete staging directory followed by an atomic rename.
# Two cold publishers may both compile, but readers see one immutable result.
set(_parallel_cache "${OUT}/parallel-cache")
set(_parallel_artifacts "${OUT}/parallel-artifacts")
file(MAKE_DIRECTORY "${_parallel_artifacts}")
string(CONCAT _parallel_command
    "\"${CMAKE_COMMAND}\" -E env \"HGL_CACHE_DIR=${_parallel_cache}\" \"HGL_ARTIFACT_DIR=${_parallel_artifacts}\" \"${HGL}\" test \"${SOURCE}\" >\"${OUT}/parallel-one.log\" 2>&1 &\n"
    "first_pid=$!\n"
    "\"${CMAKE_COMMAND}\" -E env \"HGL_CACHE_DIR=${_parallel_cache}\" \"HGL_ARTIFACT_DIR=${_parallel_artifacts}\" \"${HGL}\" test \"${SOURCE}\" >\"${OUT}/parallel-two.log\" 2>&1 &\n"
    "second_pid=$!\n"
    "wait \"$first_pid\"\n"
    "first_result=$?\n"
    "wait \"$second_pid\"\n"
    "second_result=$?\n"
    "test \"$first_result\" -eq 0 && test \"$second_result\" -eq 0\n")
execute_process(COMMAND /bin/sh -c "${_parallel_command}" RESULT_VARIABLE _parallel_result)
if(NOT _parallel_result EQUAL 0)
    file(READ "${OUT}/parallel-one.log" _parallel_one)
    file(READ "${OUT}/parallel-two.log" _parallel_two)
    message(FATAL_ERROR "concurrent cached runs failed:\n${_parallel_one}\n${_parallel_two}")
endif()
file(GLOB _parallel_entries LIST_DIRECTORIES true "${_parallel_cache}/v2/[0-9a-f]*")
list(LENGTH _parallel_entries _parallel_entry_count)
if(NOT _parallel_entry_count EQUAL 1)
    message(FATAL_ERROR "concurrent publishers produced ${_parallel_entry_count} completed entries")
endif()
file(GLOB _parallel_staging LIST_DIRECTORIES true "${_parallel_cache}/v2/.staging-*")
if(_parallel_staging)
    message(FATAL_ERROR "concurrent publication left a staging directory")
endif()

file(REMOVE_RECURSE "${OUT}")
