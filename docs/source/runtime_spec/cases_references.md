# Reference cases

Status: validated expectations with recorded variations; see [results](validation.md).

Times below are cycles from `MIN_ST`. `—` means no publication, not an empty
reference. Observe after producers and routing have evaluated. A clock input
makes the observer run even when the data input is passive or invalid.

## REF-SAMPLE — TS-1, TS-14, TS-16 and TS-17

A and B are scalar outputs. A selector publishes a reference to A, B or empty.
A REF input observes designation; a scalar input follows it. Both start invalid.

| t | A publishes | B publishes | Select | Followed value | Data modified | REF modified |
|---|---|---|---|---|---|---|
| 0 | 7 | 20 | empty | nil | false | true |
| 1 | — | — | A | 7 | true | true |
| 2 | 8 | — | — | 8 | true | false |
| 3 | — | 21 | B | 21 | true | true |
| 4 | — | — | — | 21 | false | false |
| 5 | — | — | empty | nil | false | true |
| 6 | 9 | — | A | 9 | true | true |
| 7 | — | — | — | 9 | false | false |

At 1 the input samples A: its delta is 7 and its observed last time is 1;
A's own last time remains 0. At 5 the scalar input is invalid, unmodified,
with last time `never`. The REF itself is valid: empty is a reference value.
A target publication does not evaluate the reference selector. Passive data
inputs still follow and sample; passivity only suppresses their wake.

## REF-DICTIONARY — TS-14, TS-15 and TS-19

A publishes `{X:1, Z:9}` at 0. B publishes `{Y:2, Z:3}` at 0, then `{Y:4}`
at 1. No other producer publications occur. The input selects A at 0, B at 2,
empty at 3, and A at 4. R denotes a removal in the delta.

| t | Current value | Added | Removed | Delta | Valid / modified |
|---|---|---|---|---|---|
| 0 | X:1, Z:9 | X,Z | — | X:1, Z:9 | true / true |
| 1 | X:1, Z:9 | — | — | nil | true / false |
| 2 | Y:4, Z:3 | Y | X | X:R, Y:4, Z:3 | true / true |
| 3 | nil | — | Y,Z | Y:R, Z:R | false / true |
| 4 | X:1, Z:9 | X,Z | — | X:1, Z:9 | true / true |
| 5 | X:1, Z:9 | — | — | nil | true / false |

At 2 both new children read modified with their whole value as delta,
including overlapping Z. This samples their input views, not their producers.
At 3 the withdrawal delta remains readable despite invalidity. The input has
no current children; removed children remain inspectable for that cycle.
Do not gate a withdrawal delta on `valid`.

These expectations are accepted observation by observation: Python supports
the complete removal delta; C++ supports invalidity on withdrawal. Neither
implementation's complete withdrawal row currently conforms.

## REF-EXPIRES — TS-11 and TS-23

Publish X=7 at 0. Save a reference to X's child in a separate node. Remove X at
2. At 3 write that saved reference to a REF output; this does **not** insert X.
Insert a new X=9 at 4. Write the saved reference again at 5.

A data input following the saved reference sees 7 through cycle 2, then nil
from 3 onwards. It never sees 9. The removed-child view is readable at 2 and
absent at 3. Keeping the reference does not extend the endpoint's lifetime.

The expiry rule was confirmed by the user on 2026-09-21. Python raises when
the expired reference is used; C++ retains 7 at 3 and expires it at 4.
Both are variations, not alternative permitted results.

The saved-reference case does not prove allocator slot reuse or generation
wrap handling. Those need storage tests when the Rust realization exists.
