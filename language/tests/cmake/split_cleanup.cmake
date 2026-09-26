file(REMOVE_RECURSE "${OUT}")
file(MAKE_DIRECTORY "${OUT}/src")
file(WRITE "${OUT}/src/other.part2.cpp" "unrelated module")
file(WRITE "${OUT}/src/parity.part-notes.cpp" "user source")

function(emit count)
    execute_process(COMMAND "${HGL}" emit-cpp "${SOURCE}"
        --include-dir "${OUT}/include" --src-dir "${OUT}/src" --source-parts "${count}"
        RESULT_VARIABLE result ERROR_VARIABLE error)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "emit-cpp (${count}) failed: ${error}")
    endif()
endfunction()

function(check_parts count)
    foreach(i RANGE 0 63)
        if(count GREATER 1 AND i LESS count)
            if(NOT EXISTS "${OUT}/src/parity.part${i}.cpp")
                message(FATAL_ERROR "missing part ${i} for count ${count}")
            endif()
        elseif(EXISTS "${OUT}/src/parity.part${i}.cpp")
            message(FATAL_ERROR "stale part ${i} for count ${count}")
        endif()
    endforeach()
    if(count EQUAL 1 AND EXISTS "${OUT}/src/parity.h.impl.h")
        message(FATAL_ERROR "stale private header for monolithic output")
    elseif(count GREATER 1 AND NOT EXISTS "${OUT}/src/parity.h.impl.h")
        message(FATAL_ERROR "missing private header for split output")
    endif()
endfunction()

emit(3)
check_parts(3)
# Printing a different layout must leave the existing generated files alone.
execute_process(COMMAND "${HGL}" emit-cpp "${SOURCE}" --source-parts 1
    --include-dir "${OUT}/include" --src-dir "${OUT}/src" --print
    RESULT_VARIABLE result OUTPUT_QUIET)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "printing output failed")
endif()
check_parts(3)
emit(2)
check_parts(2)
emit(1)
check_parts(1)
emit(4)
check_parts(4)
# A failed front-end pass must not remove previously generated artifacts.
file(WRITE "${OUT}/parity.hgl" "this is invalid HGL")
execute_process(COMMAND "${HGL}" emit-cpp "${OUT}/parity.hgl" --source-parts 1
    --include-dir "${OUT}/include" --src-dir "${OUT}/src"
    RESULT_VARIABLE result ERROR_QUIET)
if(result EQUAL 0)
    message(FATAL_ERROR "invalid source unexpectedly succeeded")
endif()
check_parts(4)
foreach(name IN ITEMS other.part2.cpp parity.part-notes.cpp)
    if(NOT EXISTS "${OUT}/src/${name}")
        message(FATAL_ERROR "cleanup removed unrelated source ${name}")
    endif()
endforeach()
# Refuse to delete a user-owned file even if it uses a reserved artifact name.
file(WRITE "${OUT}/src/parity.part63.cpp" "user source")
execute_process(COMMAND "${HGL}" emit-cpp "${SOURCE}" --source-parts 4
    --include-dir "${OUT}/include" --src-dir "${OUT}/src"
    RESULT_VARIABLE result ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT error MATCHES "refusing to remove unrecognized")
    message(FATAL_ERROR "cleanup should reject an unrecognized stale artifact: ${error}")
endif()
file(READ "${OUT}/src/parity.part63.cpp" preserved)
if(NOT preserved STREQUAL "user source")
    message(FATAL_ERROR "cleanup changed user-owned source")
endif()
