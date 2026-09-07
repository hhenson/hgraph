# A scripted `hgl repl` session over a pipe that stays on the direct-wiring
# backend: no runtime function or `impl fn`, so no scripted native image is
# built and the session runs on every platform. Bindings persist, composition
# declarations join the session and call each other, `eval` prints its
# sequence (with `_` padding), a temporal conditional embedded in an
# expression wires through `switch_`, and `:quit` leaves cleanly.
# `repl_smoke.cmake` adds the runtime-bearing session where the scripted
# loader exists.
#   cmake -DHGL=<hgl> -DOUT=<dir> -P repl_composition_smoke.cmake
file(MAKE_DIRECTORY "${OUT}")
file(WRITE "${OUT}/input.txt"
"let x = 1 + 2
x
fn twice(v: f64) -> f64 => v * 2.0
eval(twice, v: [1.5, 2.0])
fn quad(v: f64) -> f64 => twice(twice(v))
eval(quad, v: [1.0, _, 2.5])
fn choose(condition: bool, x: i64, y: i64) -> i64 => (if condition { x } else { y }) + 1
eval(choose, condition: [true, false], x: [1, 2], y: [10, 20])
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
foreach(expected "hgl> 3\n" "[3.0, 4.0]" "[4.0, _, 10.0]" "[2, 21]")
    string(FIND "${output}" "${expected}" at)
    if(at EQUAL -1)
        message(FATAL_ERROR "hgl repl output lacks '${expected}':\n${output}\n${errors}")
    endif()
endforeach()
