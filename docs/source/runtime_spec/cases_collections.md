# Collection cases

Status: proposed conformance cases; current validity evidence is recorded in
[Evidence](evidence.md). No runtime adapter is supplied.

## VALIDITY-IMMEDIATE — TS-9

The root is valid in every row. Each inner list has two temporal elements.
Drive real publications and invalidation to obtain these states.

| Root | Immediate children | Descendants | Root all_valid |
|---|---|---|---|
| TSL of two lists | first valid; second invalid | first only partly populated | false |
| TSL of two lists | both valid | both only partly populated | true |
| TSD of lists | live X valid | X only partly populated | true |
| TSD of lists | live X invalid | no valid value in X | false |
| TSD after removing X | no live children | removed X does not count | true |

The root must also be valid: a never-published empty collection is not
`all_valid`. This is a one-level test of children's **valid**, never their
`all_valid`. The older DictionaryDoesNotRecurse fixture had the right
non-recursion example but the wrong generalization `TSD.all_valid = valid`.
The invalid-live-child row distinguishes the two rules.

## SET-DELTA — TS-5 and TS-10

Start with an invalid `TSS<str>`. At 10, admit additions A and B: value is
`{A,B}`, added is `{A,B}`, removed is empty, valid and modified are true.
At 20, add C and remove A: value is `{B,C}`, added is `{C}`, removed is `{A}`.
At 30 with no publication the value persists, modified is false and there is
no delta. Two reads in either publishing cycle return the same changes.
Within a separate cycle, adding a previously absent D and removing it again
leaves D in neither delta set, while the set still reads modified.

## QUOTE-HISTORY — TS-1 through TS-5, TS-9, TS-11 and TS-19

Use `TSD<str, TSB{bid: TS<i64>, ask: TS<i64>}>`, initially invalid with no
keys. Only X is admitted; times increase; each cycle has at most one admitted
field publication or removal. Publishing creates X if absent. Explicit empty
insertion, field invalidation, multiple keys, same-cycle remove/reinsert,
references, failures and borrows are outside this case.

At every `begin(t)`, before its action, membership, values and all last times
are retained, modified is false, and current-cycle changes are empty. Observe
again after each listed action. At 40 there is no action.

| Time/action | X present | bid, ask | Field delta | Membership added / removed | Root / row / bid / ask last times |
|---|---|---|---|---|---|
| initial | false | —, — | — | empty / empty | never / — / — / — |
| 10: publish bid 7 | true | 7, — | bid 7 | X / empty | 10 / 10 / 10 / never |
| 20: publish ask 9 | true | 7, 9 | ask 9 | empty / empty | 20 / 20 / 10 / 20 |
| 30: publish bid 7 | true | 7, 9 | bid 7 | empty / empty | 30 / 30 / 30 / 20 |
| 40: idle | true | 7, 9 | — | empty / empty | 30 / 30 / 30 / 20 |
| 50: remove X | false | —, — | — | empty / X | 50 / — / — / — |
| 50: observe again | false | —, — | — | empty / X | 50 / — / — / — |
| 60: publish bid 7 | true | 7, — | bid 7 | X / empty | 60 / 60 / 60 / never |

`—` in row/field observations after removal means no **current** child. The
removed child remains readable through the removed view for that cycle under
TS-11; it is not a live member. This case projects current children and removal
identity, not the full removed-child accessor contract.

Initially all validity flags are false. After the first publication the root
is valid throughout, including the empty state at 50. It is modified at 10,
20, 30, 50 and 60, and unmodified at 40. A current row is valid whenever
present here; its `all_valid` is false at 10 and 60, true at 20–40, and absent
at 50. Root `all_valid` is true after the first publication: its live immediate
child is valid, or it has no live children. This does not license ignoring an
invalid live child in other cases.

Forbidden results include deriving the delta from snapshot inequality (losing
the equal bid event at 30), giving ask the bid's time, consuming removal on
read, and reviving ask when a row is reused at 60. These observations are
shared by the map and column [representation proposals](representations.md).

## MEMBERSHIP-INDEPENDENT — TS-19

Create a live key whose child has no value: it is newly added by membership,
even though its child is invalid. Publishing to that child later does not add
the key again. Invalidating the child does not remove the key. Removing the
key changes membership. Keep membership observations separate from any API's
published-value delta; [Evidence](evidence.md) names both current C++ surfaces.

The older quote fixture excluded these actions. They are preserved here as a
separate required case, not silently claimed to have been tested by it.
