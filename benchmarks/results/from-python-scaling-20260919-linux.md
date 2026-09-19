# Whole-value import scaling — before and after, 2026-09-19

`hgraph_unit_tests "[from-python-scaling]"` (`tests/cpp/test_python_user_nodes_conversion.cpp`), the
embedded-interpreter configuration (`HGRAPH_ENABLE_PYTHON_USER_NODES=ON`), Release, GCC 14, on a
private Linux validation host (x86_64). Each row replaces `n` live keys with `n` others, half of
them shared, through `python_bridge::from_python(TSDataMutationView&, ...)`, and prints the cost
per key of that one call. CLAUDE.md guardrail (iv): the cost per key must stay flat from n to 8n.

"Before" is the same test binary rebuilt with `src/hgraph/python/impl/ts_data_structured_conversions.cpp`
taken from `main` (`928b2681e`); "after" is the fix. The rows are the program's output, unedited.

## Before — a search of the listed keys per live key

```
from_python TSS n=  5000       55.3 ms   11.069 us/key
from_python TSS n= 10000      220.6 ms   22.061 us/key
from_python TSS n= 20000      903.1 ms   45.153 us/key
from_python TSS n= 40000     3634.5 ms   90.862 us/key
from_python TSD n=  5000       54.7 ms   10.949 us/key
from_python TSD n= 10000      215.7 ms   21.574 us/key
from_python TSD n= 20000      859.8 ms   42.988 us/key
from_python TSD n= 40000     3428.4 ms   85.711 us/key
```

The cost per key doubles with every doubling of `n`: 8.2x from 5k to 40k for `TSS`, 7.8x for
`TSD`, against 8.0x for a purely quadratic algorithm.

## After — one `BorrowedValueSet` of the listed keys

```
from_python TSS n=  5000        0.4 ms    0.083 us/key
from_python TSS n= 10000        0.8 ms    0.080 us/key
from_python TSS n= 20000        1.7 ms    0.083 us/key
from_python TSS n= 40000        3.3 ms    0.082 us/key
from_python TSD n=  5000        0.6 ms    0.121 us/key
from_python TSD n= 10000        1.2 ms    0.122 us/key
from_python TSD n= 20000        2.6 ms    0.128 us/key
from_python TSD n= 40000        5.2 ms    0.130 us/key
```

Flat: 0.99x from 5k to 40k for `TSS`, 1.07x for `TSD`. At 40k keys the call goes from 3.6 s to
3.3 ms (`TSS`) and from 3.4 s to 5.2 ms (`TSD`).
