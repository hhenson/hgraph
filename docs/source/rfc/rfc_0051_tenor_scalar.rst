RFC 0051: Tenor scalar
======================

:Status: Proposed, implementation under review
:Created: 2026-11-20
:Target: Native scalar transport

Contract
--------

A tenor describes a calendar adjustment as either a signed count of months
and a signed count of days, or a signed count of business days. Each count
is a 64-bit integer. Equality and hashing use the form and its counts. Zero
business days differs from zero calendar days.

C++ exposes ``Tenor::months_days(months, days)`` and
``Tenor::business_days(count)`` in ``hgraph/types/temporal.h``.
``is_business()`` identifies the form. The accessors ``months()``, ``days()``
and ``business()`` return zero for inactive components. Default construction
produces zero months and zero days. Python exposes the same factories on
``hgraph.Tenor`` and the four accessors as read-only properties. Both languages
use the registered scalar ``tenor`` and its ordinary time-series bindings.

The value contains two integers and a discriminator. It is trivially copyable.
Construction and comparison require no allocation or calendar lookup.
Applying a tenor to a date requires caller policy for holidays and month ends.
The existing ``Period`` retains its calendar arithmetic and its 32-bit counts.

Transport
---------

The JSON form has either integer ``months`` and ``days`` members or one integer
``business_days`` member. In Arrow the three nullable int64 fields have the
same names. A present struct sets exactly the fields of one form.

Binary records use tag byte 0 followed by signed months and days, or tag byte
1 followed by signed business days. Each count uses zigzag-encoded LEB128.
The canonical manifest assigns the scalar wire identity 18.

Adoption and validation
-----------------------

The `compiler implementation`_ already transports both forms in a downstream
scalar. After release the compiler can delegate storage to ``Tenor`` and retain
its own calendar operations. Native and Python applications share the same
recorded values.

Native and Python tests exercise both forms, the signed limits, hashing,
wiring and repeated publications. Codec tests cover JSON, binary and both
table writers. An installed C++ SDK consumer verifies registration and
round-trip through an owned scalar.

.. _compiler implementation: https://github.com/hhenson/hgl-compiler/pull/153
