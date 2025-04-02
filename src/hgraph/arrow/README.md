Arrow API
=========

The HGraph Arrow API provide an Arrow style monoid approach to writing HGraph
code.

The basic data-type structure is the use of a time-series value or tuples of time-series values, that is
``TIME_SERIES_TYPE`` | ``tuple[A, B]``, where `A`, `B` can be time-series values or time-series tuples.

To construct the tuples use the ``arrow`` function to build the tuple inputs, for example:
```python
    arrow(const(1))  # Single value
    arrow(const(1), const(2))  # A pair
    arrow((const(1), const(2)), const(3))  # A pair with first being a tuple and the second a time-series.
```

For simplicity’s sake, we support dropping the const wrapper for constants that are
TS[SCALAR].

i.e. ``arrow(1)``, ``arrow(1, 2)``, and ``arrow((1, 2), 3)``

A ``lambda`` function (or ``graph`` or ``compute_node``) can also be wrapped using the arrow
wrapper function. This uplifts the function to be ``arrow`` capable.

The value is then presented to the arrow functions using the ``|`` operator.

For example:
```python
    
    arrow(1) | arrow(lambda x: debug_print("Out", x))
```

Arrow functions are designed to be chained together using the ``>>`` operator, for example:

```python
arrow(1, 2) | i >> (lambda pair: pair[0] + pair[1]) >> (lambda x: debug_print("out", x))
```

NOTE: Only the first function in the chain must be an arrow wrapped function, in this example
we use the ``identity`` arrow function (shortened to ``i``) to initiate the chain. The ``identity``
function is a function that does nothing and is useful when we wish to perform a no operation.
It is also great to initiate the pipe allowing us to avoid constantly having to wrap the
following functions with the ``arrow`` wrapper.

It is also possible to call an arrow function chain using normal calling semantics, for 
example:

```python
sum_and_print = i >> lambda pair: pair[0] + pair[1] >> lambda x: debug_print("out": x)
sum_and_print(TSL.from_ts(const(1), const(2)))
```

Given the large focus on tuples, there are two other overloads we provide, firstly:

'/' - This is the fan-out operator. This will take a single input and apply it to two functions
      that are on either side of this operator, for example:

```python
arrow(1) | i / i >> (lambda x: debug_print("out", x))

>>> out: {0: 1, 1: 1}
```

'//' - This is the cross operator, it expects a tuple and splits the tuple
       sending the first to left function and the second to the right function, 
       for example:

```python
arrow(1, 2) | arrow(lambda x: x+1) // (lambda x: x+2) >> (lambda x: debug_print("out", x))

>>> out: {0: 2, 1: 4}
```

There are a couple of important base operators that help build a baseline for processing 
arrow flows. These include:

``first`` -  Selects the first element of a tuple

``second`` - Selects the second element of a tuple

``swap`` - Swaps the items in a pair, so [1, 2] -> [2, 1]

``binary_op`` - Utility function to wrap a binary node / graph, this splits
           the pair and applies the values to the binary operator.

``assoc`` - Adjust the associativity of a tuple of tuples, that is:
            arrow((1, 2), 3) >> assoc  produces a value of (1, (2, 3))

``apply_`` - Applies a function in the first of the tuple to the value in the second of the tuple.


Then there are a few helper components:

``assert_`` - Performs an assertion on the stream. The value is passed through,
              so it is possible to chain multiple assertions at various
              steps in the evaluation chain.

``eval_`` - Wraps up a set of scalar values, including lists which will
            be converted into a time-series of values and when applied
            to the arrow function chain it will evaluate the graph, returning
            the collected results as an array, similar to ``eval_node`` 
            with ``__elide__`` set to ``True``.