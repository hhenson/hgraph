# A bounded atomic layout example

Status: proposed physical exercise preserved from #934; not the size, ABI,
serialization format or chosen representation of a hgraph endpoint.

## Placement

Assume C++ object storage, 8-bit bytes and a 64-bit size limit. A payload's
target-verified size s and alignment a must be positive; a is a power of two
and s is a multiple of a. Put the payload at 0 and an eight-byte, eight-aligned
stamp at `align_up(s, 8)`. Region alignment is `max(a, 8)` and size is
`align_up(stamp_offset + 8, region_alignment)`. `align_up(n,a)` is the least
multiple of a not smaller than n.

Compute mathematically, then reject any offset, end or size above `2^64 - 1`
before allocation. Invalid traits produce InvalidStorage; overflow produces
LayoutOverflow; overlapping/misaligned regions produce InvalidLayout. These
are example planning categories, not current native error names.

| Payload size/alignment | Payload offset | Stamp offset | Region size/alignment |
|---|---|---|---|
| 8 / 8 | 0 | 8 | 16 / 8 |
| 1 / 1 | 0 | 8 | 16 / 8 |
| 32 / 16 | 0 | 32 | 48 / 16 |
| 8 / 3 | — | — | InvalidStorage |
| 18446744073709551608 / 8 | — | — | LayoutOverflow |
| pair of 8 / 8 cells | left 0; right 16 | local stamp offset 8 each | 32 / 8 |

The containing owner supplies aligned, stable storage from successful
construction through destruction. Construct live C++ objects before typed
access, destroy before release, and never let a field free its containing
region. Padding has no meaning. Neither a native struct's offsets nor byte
copying as object relocation follows from this plan. Generic payload failure
and lifetime rules still need a separate contract.

## Relating an i64 cell to atomic behaviour

For the 8/8 payload only, construct payload zero and stamp minus one. A stamp
of minus one decodes to no current value and `never`; a nonnegative stamp in
`[0, 2^63 - 1]` decodes to a real abstract tick and a present payload. Decode
zero and the maximum explicitly in boundary checks. Other negative stamps
are outside this profile. Observation time comes from the external context.

On admitted publication assign payload, then stamp; exclude concurrent reads
and observe after both assignments. Begin changes only the context. Run both
[atomic cases](cases_atomic.md) unchanged. No valid flag, modified flag,
current-time field or ops pointer lives in this region. Publication and
inspection allocate no **cell** storage; owner and observer infrastructure is
excluded from the footprint. No total endpoint memory saving is claimed.

The [pair cases](cases_lifecycle.md) add owner, borrow and construction-failure
obligations without moving the controller into these 32 bytes. Physical
validation needs actual target size/alignment assertions, checked boundaries,
object lifetime tests and sanitizers. Arithmetic consistency in this document
is not that native evidence.
