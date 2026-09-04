# A scripted `hgl repl` session over a pipe (the non-terminal path of the
# line reader): bindings persist, a declaration joins the session, `eval`
# prints its sequence, and `:quit` leaves cleanly.
#   cmake -DHGL=<hgl> -DOUT=<dir> -P repl_smoke.cmake
file(MAKE_DIRECTORY "${OUT}")
file(WRITE "${OUT}/input.txt"
"let x = 1 + 2
x
fn twice(v: f64) -> f64 => v * 2.0
eval(twice, v: [1.5, 2.0])
:quit
")
execute_process(
    COMMAND "${HGL}" repl
    INPUT_FILE "${OUT}/input.txt"
    OUTPUT_VARIABLE output
    ERROR_VARIABLE errors
    RESULT_VARIABLE status
)
if(NOT status EQUAL 0)
    message(FATAL_ERROR "hgl repl exited ${status}\n${output}\n${errors}")
endif()
foreach(expected "hgl> 3\n" "[3.0, 4.0]")
    string(FIND "${output}" "${expected}" at)
    if(at EQUAL -1)
        message(FATAL_ERROR "hgl repl output lacks '${expected}':\n${output}\n${errors}")
    endif()
endforeach()
