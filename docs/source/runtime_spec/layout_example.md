# Atomic layout example

Status: proposed cell storage; no endpoint-size, ABI or serialization claim.

## Placement

C++ storage, 8-bit bytes, 64-bit size limit. Payload size s and alignment a
are target-verified and positive; a is a power of two and divides s. Payload
offset is 0; the eight-byte stamp is at `align_up(s, 8)`. Alignment is
`max(a, 8)`; size is `align_up(stamp_offset + 8, alignment)`. Align-up gives
the least multiple at or above its first argument.

Compute mathematically; reject before allocation: InvalidStorage for bad
traits, LayoutOverflow for any offset/end/size above `2^64 - 1`, InvalidLayout
for overlap or misalignment. These are example error categories.

| Payload size/alignment | Payload offset | Stamp offset | Region size/alignment |
|---|---|---|---|
| 8 / 8 | 0 | 8 | 16 / 8 |
| 1 / 1 | 0 | 8 | 16 / 8 |
| 32 / 16 | 0 | 32 | 48 / 16 |
| 8 / 3 | — | — | InvalidStorage |
| 18446744073709551608 / 8 | — | — | LayoutOverflow |
| pair of 8 / 8 cells | left 0; right 16 | local stamp offset 8 each | 32 / 8 |

The owner holds aligned, stable storage until destruction. Construct before
typed access; destroy before release. Fields do not free their region. Padding
has no meaning; native struct offsets and byte relocation are not implied.
Generic payload lifetime and failure need their own contract.

## The i64 realization

For the 8/8 payload, construct value 0 and stamp -1. Stamp -1 means absent and
`never`; `[0, 2^63 - 1]` means a real abstract tick and present value. Check zero
and the maximum; other negative stamps are excluded. Time comes from context.

Publish writes payload then stamp; no concurrent reads. Observe after both.
Begin changes only context. Run the [atomic cases](cases_atomic.md) unchanged.
The region holds no flags, current time or ops pointer. Publish and inspect
allocate no cell storage; owner and observers are outside the size count.

The [pair cases](cases_lifecycle.md) add lifetime obligations. Physical proof
needs target assertions, boundary and lifetime checks, and sanitizers.
