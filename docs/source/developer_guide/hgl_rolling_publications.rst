HGL rolling publications
========================

A ``rolling<V, Max, Min>`` publication carries one complete ordinary ``V``
arrival. Its delta is ``V``, while the endpoint and generic origin preserve
window kind, both limits, and the exact payload type. The first arrival is
valid and modified even before the window is ready; forwarding does not
require ``all_valid``. Equal arrivals remain distinct publications.

Generated forwarding applies each arrival to the output's own window at the
output publication time. Tick windows retain at most ``Max`` arrivals;
duration windows evict arrivals older than ``Max`` only on a new arrival.
An arrival exactly ``Max`` old remains in the window. Silence causes neither
eviction nor publication. Readiness reflects the current count or retained
time span and can become false again after a long gap.

Eval preflights complete ordinary payloads. Replay and record retain owning
arrival snapshots and their actual publication times, including list and
struct payloads. No rolling-delta constructor or ordinary rolling value is
introduced. The pinned ``rolling-publications.md`` specification defines the
admitted profile.
