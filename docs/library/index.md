HGraph Library
==============

Standard operators for time series.
---------

These are the python operators available for time series out of the box and most of them will do the same as builtin 
python operators would, for example `a + b` where a and b are time series of `int` will produce a time series of `int` 
that will be updated every time `a` or `b` is updated with the value that is equal to the sum of values in `a` and `b`. 

Note some operators in Python cannot be overloaded,
(e.g. `and` and `or`) so for those operations nodes with underscore at the end are used e.g. `and_` for `and`.

| operator       | description      | hgraph name | TS[int] TS[float]    | TS[bool]            | TS[str]                | TS[enum]            | TS[date,datetime]        | TS[timedelta]          | TS[CompoundScalar] | TS[tuple]                 | TS[set]              | TS[dict]              | TSS[T]                 | TSL[TST, S]           | TSB[Schema]             | TSD[K, V]                          |
|----------------|------------------|:------------|----------------------|---------------------|------------------------|---------------------|--------------------------|------------------------|--------------------|---------------------------|----------------------|-----------------------|------------------------|-----------------------|-------------------------|------------------------------------|
| a + b          | addition         | add_        | &#9989;              | &#10060;            | &#9989;                | &#10060;            | &#9989; b is timedelta   | &#9989;                | &#10060;           | &#9989; concat tuples     | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a - b          | subtraction      | sub_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#9989;                  | &#9989;                | &#10060;           | &#10060;                  | &#9989; set diff     | &#9989; diff          | &#9989; set difference | &#9989; item-wise     | &#9989; item-wise       | &#9989; diff                       |
| a * b          | multiplication   | mul_        | &#9989;              | &#10060;            | &#9989; b is int       | &#10060;            | &#10060;                 | &#9989; b is int/float | &#10060;           | &#9989; b is int          | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a / b          | true division    | div_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#9989; b is int/float | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a // b         | floor division   | floordiv_   | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a % b          | modulo           | mod_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| divmod(a, b)   | divmod           | divmod_     | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#10060;              | &#10060;                | &#10060;                           |
| a ** b         | pow              | pow_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a << b         | left shift       | lshift_     | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10067; shift items left | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a >> b         | right shift      | rshift_     | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a & b          | bitwise and      | bit_and     | &#9989;              | &#9989;             | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#9989; intersection | &#10067; intersection | &#9989; intersection   | &#9989; item-wise     | &#9989; item-wise       | &#9989; intersection               |
| a &#124; b     | bitwise or       | bit_or      | &#9989;              | &#9989;             | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#9989; union        | &#10067; union        | &#9989; union          | &#9989; item-wise     | &#9989; item-wise       | &#9989; merge                      |
| a ^ b          | bitwise xor      | bit_xor     | &#9989;              | &#9989;             | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#9989; symm. diff   | &#10067; symm. diff   | &#9989; symm. diff     | &#9989; item-wise     | &#9989; item-wise       | &#9989; symm. diff                 |
| a == b         | equal            | eq_         | &#9989;              | &#9989;             | &#9989;                | &#9989;             | &#9989;                  | &#9989;                | &#9989;            | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#9989; single bool   | &#9989; single bool     | &#9989;                            |
| a != b         | not equal        | ne_         | &#9989;              | &#9989;             | &#9989;                | &#9989;             | &#9989;                  | &#9989;                | &#9989;            | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#9989; single bool   | &#9989; single bool     | &#9989;                            |
| a < b          | less             | lt_         | &#9989;              | &#9989;             | &#9989;                | &#9989; value       | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#10060;              | &#10060;               | &#10060;              | &#10060;                | &#10060;                           |
| a <= b         | less or equal    | le_         | &#9989;              | &#9989;             | &#9989;                | &#9989; value       | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#10060;              | &#10060;               | &#10060;              | &#10060;                | &#10060;                           |
| a > b          | greater          | gt_         | &#9989;              | &#9989;             | &#9989;                | &#9989; value       | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#10060;              | &#10060;               | &#10060;              | &#10060;                | &#10060;                           |
| a >= b         | greater or equal | ge_         | &#9989;              | &#9989;             | &#9989;                | &#9989; value       | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#10060;              | &#10060;               | &#10060;              | &#10060;                | &#10060;                           |
| -a             | unary negative   | neg_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#9989;                | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| +a             | unary positive   | pos_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#9989;                | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| ~a             | inversion        | invert_     | &#9989;              | &#9989;             | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| abs(a)         | abs              | abs_        | &#9989;              | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#9989;                | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| a[b]           | getitem          | getitem_    | &#10060;             | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#9989;                   | &#10060;             | &#9989;               | &#10060;               | &#9989; item by index | &#9989; item by index   | &#9989;                            |
| a.b            | getattr          | getattr_    | &#10060;             | &#10060;            | &#10060;               | &#10060;            | &#10060;                 | &#10060;               | &#9989;            | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#10060;              | &#9989; item by name    | &#9989; if values is a schema type |
| &nbsp;         |                  |             |                      |                     |                        |                     |                          |                        |                    |                           |                      |                       |                        |                       |                         |                                    |
| not_(a)        | not              | not_        | &#9989;              | &#9989;             | &#9989;                | &#10060;            | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#10060;              | &#10060;                | &#9989;                            |
| and_(a, b)     | and              | and_        | &#9989;              | &#9989;             | &#9989;                | &#10060;            | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#10060;              | &#10060;                | &#10060;                           |
| or_(a, b)      | or               | or_         | &#9989;              | &#9989;             | &#9989;                | &#10060;            | &#9989;                  | &#9989;                | &#10060;           | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#10060;              | &#10060;                | &#10060;                           |
| len_(a)        | len              | len_        | &#10060;             | &#10060;            | &#9989;                | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#9989;               | &#9989; number of items | &#9989;                            |
| &nbsp;         |                  |             |                      |                     |                        |                     |                          |                        |                    |                           |                      |                       |                        |                       |                         |                                    |
| min_(*args)    | multi-arg min    | min_        | &#9989;              | &#9989;             | &#9989;                | &#9989; value       | &#9989;                  | &#9989;                | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| max_(*args)    | multi-arg max    | max_        | &#9989;              | &#9989;             | &#9989;                | &#9989; value       | &#9989;                  | &#9989;                | &#10060;           | &#10060;                  | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| sum_(*args)    | multi-arg sum    | sum_        | &#9989;              | &#9989;             | &#9989;                | &#10060;            | &#10060;                 | &#9989;                | &#10060;           | &#9989;                   | &#10060;             | &#10060;              | &#10060;               | &#9989; item-wise     | &#9989; item-wise       | &#10060;                           |
| min_(a)        | unary min        | min_        | &#9989; running min  | &#9989; running min | &#9989; running min    | &#9989; running min | &#9989; running min      | &#9989; running min    | &#10060;           | &#9989; min item          | &#9989; min item     | &#9989; min value     | &#9989; min item       | &#9989; min item      | &#9989; min item        | &#9989; min value                  |
| max_(a)        | unary max        | max_        | &#9989; running max  | &#9989; running max | &#9989; running max    | &#9989; running max | &#9989; running max      | &#9989; running max    | &#10060;           | &#9989; max item          | &#9989; max item     | &#9989; max value     | &#9989; max item       | &#9989; max item      | &#9989; max item        | &#9989; max value                  |
| sum_(a)        | unary sum        | sum_        | &#9989; running sum  | &#9989; running sum | &#9989; running sum    | &#10060;            | &#10060;                 | &#9989; running sum    | &#10060;           | &#9989; sum items         | &#9989; sum items    | &#9989; sum values    | &#9989; sum items      | &#9989; sum items     | &#9989; sum items       | &#9989; sum value                  |
| &nbsp;         |                  |             |                      |                     |                        |                     |                          |                        |                    |                           |                      |                       |                        |                       |                         |                                    |
| contains(a, b) | b in a           | contains_   | &#10060;             | &#10060;            | &#9989; substring find | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#9989;                   | &#9989;              | &#9989; key find      | &#9989;                | &#10060;              | &#10060;                | &#9989;                            |
| is_empty(a)    | empty test       | is_empty    | &#10060;             | &#10060;            | &#9989;                | &#10060;            | &#10060;                 | &#10060;               | &#10060;           | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#10060;              | &#10060;                | &#9989;                            |
| type_(a)       | type             | type_       | &#9989; int or float | &#9989; bool        | &#9989; str            | &#9989; enum        | &#9989; date or datetime | &#9989; timedelta      | &#9989;            | &#9989; tuple[...]        | &#9989; set[...]     | &#9989; set[...]      | &#10060;               | &#10060;              | &#10060;                | &#10060;                           |
| str_(a)        | str              | str_        | &#9989;              | &#9989;             | &#9989; no-op          | &#9989;             | &#9989;                  | &#9989;                | &#9989;            | &#9989;                   | &#9989;              | &#9989;               | &#9989;                | &#9989;               | &#9989;                 | &#9989;                            |
| &nbsp;         |                  |             |                      |                     |                        |                     |                          |                        |                    |                           |                      |                       |                        |                       |                         |                                    |

Time series conversion 
----------------------

### const
Ticks the provided scalar value at the start time of the graph as a time series of given type. The result time series 
only ever tick once. Examples are `const(1)`, `const((1, 2))`, `const[TSD[int, TS[int]]]({1: 2, 3: 4})`. `const` defaults
the time serise type to simple time series of the type of the scalar value provided so `const(1)` will be `TS[int]`,
`const((1, 2))` will be `TS[Tuple[int, int]]`. Complex time series types require type to be provided explicitly. 

### convert
This operation converts between different types of timeseries. Its syntax is `convert[TO_TS_TYPE](ts)`. See the 
compatibility matrix:

| from \ to              | TS[T], T is int, float | TS[bool] | TS[str] | TS[CompoundScalar] | TS[tuple]            | TS[set]              | TS[dict]                     | TSS[T]             | TSL[TST, S] | TSB[Schema]                 | TSD[K, V]                                                    |
|------------------------|------------------------|----------|---------|--------------------|----------------------|----------------------|------------------------------|--------------------|-------------|-----------------------------|--------------------------------------------------------------|
| TS[T], T is int, float | &#9989;                | &#9989;  | &#9989; |                    | &#9989; tuple of one | &#9989; tuple of one | &#9989; key and value inputs | &#9989; set of one |             |                             |                                                              |
| TS[bool]               | &#9989;                | &#9989;  | &#9989; |                    | &#9989; tuple of one | &#9989; tuple of one |                              |                    |             |                             |                                                              |
| TS[str]                | &#9989;                | &#9989;  | &#9989; |                    | &#9989; tuple of one | &#9989; tuple of one |                              | &#9989; set of one |             |                             |                                                              |
| TS[CompoundScalar]     | &#10060;               | &#9989;  | &#9989; | &#9989;            |                      |                      |                              |                    |             | &#9989;                     |                                                              |
| TS[tuple]              | &#10060;               | &#9989;  | &#9989; |                    | &#9989;              | &#9989;              | &#9989; key and value tuples | &#9989;            | &#9989;     |                             |                                                              |
| TS[set]                | &#10060;               | &#9989;  | &#9989; |                    | &#9989;              | &#9989;              |                              | &#9989;            |             |                             | &#9989; if supplied a value                                  |
| TS[dict]               | &#10060;               | &#9989;  | &#9989; |                    |                      |                      | &#9989;                      |                    |             | &#9989; for uniform schemas | &#9989;                                                      |
| TSS[T]                 | &#10060;               | &#9989;  | &#9989; |                    | &#9989;              | &#9989;              |                              |                    |             |                             | &#9989; if supplied a value                                  |
| TSL[TST, S]            | &#10060;               | &#9989;  | &#9989; |                    | &#9989;              |                      | &#9989; ints for keys        |                    | &#9989;     |                             | &#9989; with int keys, or use combine with a tuple of keys |
| TSB[Schema]            | &#10060;               | &#9989;  | &#9989; | &#9989;            |                      |                      |                              |                    |             | &#9989;                     | &#9989; if the schema is uniform                             |
| TSD[K, V]              | &#10060;               | &#9989;  | &#9989; |                    |                      |                      | &#9989;                      |                    |             | &#9989; for uniform schemas | &#9989;                                                      |

### combine
Combine take some time series and combines them into a collection time series. For example, `combine[TS[Tuple[int, int]]](a, b)`.
The following types are supported:

| type              | syntax                                   |
|-------------------|------------------------------------------|
| TS[Tuple[T, ...]] | combine[TS[Tuple]](a, b, c)              |
| TS[Tuple[T1, T2]] | combine[TS[Tuple[T1, T2]]](a, b, c)      |
| TSL[T, SIZE]      | combine[TSL](a, b, c)                    |
| TSS[T]            | combine[TSS](a, b, c)                    |
| Named Schema      | combine[TSB[SchemaClass]](a=a, b=b, c=c) |
| Unnamed TS Schema | combine[TSB](a=a, b=b, c=c)              |
| TSD[K, V]         | combine[TSD](keys=<tuple of 3>, a, b, c) |

### collect
This operation collects values into a collection, Syntax `collect[TSS[int]](ts)`

| from \ to                         | TS[tuple]            | TS[set] | TS[Mapping]                         | TSS[T]  | TSL[TST, S] | TSB[Schema] | TSD[K, V]                       |
|-----------------------------------|----------------------|---------|-------------------------------------|---------|-------------|-------------|---------------------------------|
| TS[T], T is int, float, bool, str | &#9989;              | &#9989; | &#9989; from two inputs for K and V | &#9989; |             |             | &#9989; for two inputs, K and V |
| TS[tuple]                         | &#9989; same as sums | &#9989; | &#9989; from two inputs for K and V | &#9989; |             |             | &#9989; for two inputs, K and V |
| TS[set]                           |                      |         |                                     | &#9989; |             |             |                                 |
| TS[Mapping]                       |                      |         |                                     |         |             |             | &#9989;                         |

`collect` can also receive a `remove` input to tell it to remove items from the collection.

### emit

#### Purpose
`emit` is the opposite of `collect` and applies to collection types to release their values one by one with the given 
interval. If collection gets updated the new values are queued up behind the previous ones. This operator does not require
type arguments as it can derive the output type from the input arguments.

#### Example

```python
from hgraph.nodes import emit, print

values = emit( (1, 2, 3) )
print(values)

>> 1
>> 2
>> 3
```


| input type             | output type |
|------------------------|-------------|
| TS[tuple[SCALAR, ...]] | TS[SCALAR]  |
| TS[set[SCALAR]]        | TS[SCALAR]  |
| TS[Mapping[K, V]]      | TS[V]       |
| TSS[SCALAR]            | TS[SCALAR]  |
| TSL[T, SIZE]           | T           | 
| TSD[K, V]              | V           |


Stream operators
----------------
Stream operators apply to any type of time series and are designed to aid management of flow of ticks in time series.

### schedule
`schedule` allows to generate regular ticks in the graph. For example `schedule(timedelta(seconds=3))` will produce a 
time series of type `TS[bool]` that will tick `True` every three seconds. THe `initial_delay` parameter regulates 
delay before the first tick and `max_ticks` limits the total number of ticks produced.

### sample 
`sample` allows to snap the value of a time series on a tick from another time series.

### resample
`resample` is `sample` done on a regular schedule - for example it can snap the value of a time series every second. 
This is useful to regularise time series before applying statistical functions for example.   

### lag
`lag` delays values of a time series by either specified time or by a number of ticks. In the latter case with lag of 1 
it will tick the previous value of the time series every time the series tick. 

### dedup
`dedup` removes duplicates form a time series - if its input ticks the same value again the new tick is not propagated. 
This is useful to avoid processing the same value more than once if the algorithm the graph implements would produce 
the same result.  

### filter_
`filter_` suppresses ticks of a time series when the `condition` time series' value is `False`.

### throttle
`throttle` reduces the rate of ticks in a time series to the given period. It works like `resample` if the rate of 
ticks is higher than the period but unlike `resample` does not produce ticks when the source time series does not tick. 

### gate
`gate` queues up ticks of a time series when the value of `condition` if `False`. Once it turns `True` the queued up 
ticks are released one by one with the given delay between them.  

### batch
`batch` works like `gate` but releases queued up ticks in batches into `TS[Tuple[T, ...]]` output for `TS[T]` input. 

### take
`take` filters out all ticks the input time series after `n` initial ticks or the given timedelta.   

### drop
`drop` filters out `n` initial ticks of the input time series or does it for a given timedelta.

### step
`step` lets through only every `n`th tick of the input time series 

### slice
`slice` combines `drop`, `take` and `step` into one operation. It works like pythons slice indexing but along the ticks
or time axis. 

### window
`window` is a rolling window function for time series - for every tick of the input time series it outputs a tuple of 
previous values of the given size or time.

Flow control
------------

### if_true
Emits a tick with the value True when the input condition ticks with the value ```True```.  
If `tick_once_only` is true then the node will not tick more than once.  
Otherwise the node will tick whenever the condition ticks ```True```. 

### if_
`if_` forwards a time series to either the `true` or `false` element of the output bundle, according to whether
`condition` is `True` or `False`.

### route_by_index
`route` does the inverse of `if_` and `TSL[]` - based on an index forward one input time series to one of its outputs 
with the matching index. If the index is `bool` then the outputs number is 2. 

### if_then_else
`if_then_else` forwards either its `true` input or `false` input based on the value of `condition`

### TSL[]
TSL indexing operator (where index is a timeseries of `int`) forwards a time series based on the index - therefore
extending what `if_then_else` does to any number of inputs. 

### index_of
`index_of` is useful in conjunction with the TSL indexing operator to work out the index based on a number of other 
inputs. For example, given five `TS[bool]` inputs, `index_of` can be used to find one with `True` value and give its 
index - which then can be given to `TSL[]` to pick the correct time series to forward. 

### merge
`merge` forwards the last ticked value from its inputs. If two inputs tick at the same time the leftmost is forwarded

### race
`race` is like `merge` but it forwards only values from the input that become valid first. If it becomes invalid `race`
moves on to the next valid input.

### all_
Ticks with True when all the args are True.

### any_
Ticks with True when any of the args is True


Stream analytical operators
---------------------------

Note - `mean`, `std` and `var` have the same semantics as `min`, `max` and `sum`
* If given a single scalar argument they return a running operation on the timeseries.
* If given a single collection argument they return the operation on the latest timeseries value
* If given multiple arguments they calculate the operation across the latest values of the given timeseries

### mean
Calculates the mean

### std
Calculates the standard deviation

### var
Calculates the variance

### diff
`diff` returns a time series of difference between the previous value of a time series and the current

### count
`count` counts the number of ticks of a time series

### clip
`clip` produces a time series of the values from its input restricted to the given range 

### quantiles
`quantiles` calculates quantiles of the values from ints input and ticks the quantiles calculated when `trigger` input 
ticks (_not yet implemented_)

### ewma
exponentially weighted mean

### ewstd
exponentially weighted standard deviation (_not yet implemented_)


TSD operators
-------------
Note: these also apply to `TS[dict]` timeseries and have same semantics 

### keys_
Returns a TSS (or set) of keys in the dictionary

### values_
Returns a tuple of the values in the dictionary. Note: does not apply to TSD as there is no suitable time series type

### rekey
`rekey` receives a `TSD[K, V]` and a mapping `TSD[K, K1]` and remaps the keys using it so that its output is `TSD[K1, V]` 

### flip
`flip` produces a TSD where the keys are values from the input TSD and the values are keys from the input.

### partition
`partition` splits a TSD into multiple TSDs give a mapping TSD. Its output is a TSD[K1, TSD[K, V]] for inputs of 
TSD[K, V] and TSD[K, K1] for mapping

### flip_keys
`flip_keys` work on nested TSDs like TSD[K, TSD[K1, V]] to inverse keys to get TSD[K1, TSD[K, V]]

### collapse_keys
Given a nested TSD[K, TSD[K1, V]] `collapse_keys` will produce TSD[Tuple[K, K1], V] where the keys are pairs of outer 
and inner key for each value 

### uncollapse_keys
`uncollapse_keys` deos the reverse operation to `collapse_keys`


Str operators
-------------

### match_
`match_` does regex pattern matching

### replace
`replace` replaces all occurrences of a pattern in a string with another string

### split
`split` splits a string into a tuple of strings based on a separator

### join
`join` joins a tuple of strings into a single string using the given separator

### format_
`format_` formats strings given the format spec


Graph operators
----------------

### default
ticks provided scalar value if the provided time series is not valid 

### nothing
never produces a tick

### null_sink
consumes a time series and does nothing

### valid
returns a bool time series that indicates if the input is valid (i.e. has a value)

### last_modified_time
returns a datetime timeseries that ticks the last modified time of its input

### last_modified_date
returns a date timeseries that ticks the last modified date of its input

### zero
overloads of this operator provide zero value time series for the reduce_ operator

### log
logs into the system logger 

### print_
prints into the system output