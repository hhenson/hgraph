# Collection cases

Status: proposed. See [Evidence](evidence.md) for current validity tests.

## VALIDITY-IMMEDIATE — TS-9

The root is valid; each inner list has two elements.

| Root | Immediate children | Descendants | Root all_valid |
|---|---|---|---|
| TSL of two lists | first valid; second invalid | first only partly populated | false |
| TSL of two lists | both valid | both only partly populated | true |
| TSD of lists | live X valid | X only partly populated | true |
| TSD of lists | live X invalid | no valid value in X | false |
| TSD after removing X | no live children | removed X does not count | true |

Check the root's validity and immediate children's **valid**, never their
`all_valid`. A never-published empty collection fails. The invalid-child row
also rules out the older shortcut `TSD.all_valid = valid`.

## SET-DELTA — TS-5 and TS-10

Start with an invalid `TSS<str>`. At 10 add A and B: value and added are
`{A,B}`, removed is empty, valid and modified are true. At 20 add C and remove
A: value `{B,C}`, added `{C}`, removed `{A}`. Idle at 30: retain the value,
unmodified, no delta. Repeated reads agree. In a separate cycle, adding then
removing a new D leaves it in neither delta set; the set still ticks.

## QUOTE-HISTORY — TS-1 through TS-5, TS-9, TS-11 and TS-19

`TSD<str, TSB{bid: TS<i64>, ask: TS<i64>}>`, initially invalid and empty.
Only X; at most one field publication or removal per cycle. Publication creates
X if absent. Empty insertion, field invalidation, multiple keys, same-cycle
remove/reinsert, references, failures and borrows are excluded.

Begin retains membership, values and last times; modified is false and changes
are empty. Read after each action below; 40 is idle.

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

After removal, `—` means no current child. Both reads at 50 see the removed
child `{bid: 7, ask: 9}`, with row/bid/ask times `30 / 30 / 20` (TS-11).
That removed view expires at the cycle boundary; reinsertion at 60 is fresh.

Initially all flags are false. Thereafter the root is valid and all_valid,
including when empty; modified is true except at 40. A present row is valid;
its all_valid is false at 10 and 60, true at 20–40, and absent at 50.

Both [representations](representations.md) must retain the equal tick at 30,
the ask's own time, repeated removal reads, and a fresh ask at 60.

## MEMBERSHIP-INDEPENDENT — TS-19

Insert a key with an invalid child: the key is added. Publishing or invalidating
that child later neither adds nor removes the key. Removing the key does.
Replay needs both membership changes and published-value deltas (TS-5).
[Evidence](evidence.md) names the C++ surfaces. This case extends beyond
QUOTE-HISTORY.
