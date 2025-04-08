Arrow API
=========

The HGraph Arrow API provide an Arrow style monoid approach to writing HGraph
code. This is based on the paper "A new notation for arrows" by Ross Paterson (2001).

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

``/`` - This is the fan-out operator. This will take a single input and apply it to two functions
      that are on either side of this operator, for example:

```python
arrow(1) | i / i >> (lambda x: debug_print("out", x))

>>> out: {0: 1, 1: 1}
```

``//`` - This is the cross operator, it expects a tuple and splits the tuple
       sending the first to left function and the second to the right function, 
       for example:

```python
arrow(1, 2) | arrow(lambda x: x+1) // (lambda x: x+2) >> (lambda x: debug_print("out", x))

>>> out: {0: 2, 1: 4}
```

``+arrow_fn`` - Apply the left tuple entry to the function, this is equivalent to: `` >> arrow_fn // i``

``-arrow_fn`` - Apply to the right tuple entry to the function, this is equivalent to: ``>> i // arrow_fn``

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

``const_`` - Introduce a constant into the flow. This is useful when binding constant values into binary 
             operators such as ``eq_``.

``<<`` - This adds the ability to 'bind' a result to a binary operator, this makes:

    >> eq_ << const(1)

Goes to:

    >> i / const(1) >> eq_

It does make the code look a bit cleaner with slightly less code required.

Next there are a number of standard operators that can be used such as: ``eq_``, etc.

Next for control flow we have:

``if_(condition).then(fn1).otherwise(fn2)`` - This is the full version, the condition function or arrow takes
    the input and produces a binary stream. The functions fn1 and fn2 take the input stream (prior to if) and
    must convert to the same result shape. But otherwise when the condition is ``True`` then ``fn1`` is evaluated
    and then ``fn2`` is evaluated when the condition is ``False``.
    This can be short-cut to ``if_(conditions).then(fn1)``, in the case when ``False`` no values are produced, when
    ``True`` the ``fn1`` is evaluated.

``if_then(fn1).otherwise(fn2)`` - In this scenario the ``if_`` clause is not used, instead the operator expects a 
    pair with the first element being a bool value used for the condition.

``fb[<label>: <type>, "default": SCALAR, "passive": True]`` - Initiates a feedback, the label must be unique.
   The output is a pair of input and the feedback output. If default is provided then the first tick will be 
   the default value. When passive is selected the feedback will only tick when the input ticks.

``fb[<label>]`` - Consumes the value into the feedback, emits the value it consumes

``switch_(<option_map>)`` - provide a pattern matched (to the mapping keys) optional set of 
     control flows. The ``option_map`` is a dictionary of key to arrow function.
     the switch_ expects a pair, where the first value of the pair is used to select the graph
     and the second is supplied to the option selected for evaluation.

``map_(<fn>)`` - processes a collection time-series such as a TSL or TSD applying the
   arrow function ``fn`` to each of the values associated to each value of the collection.

``reduce(<fn>, <zero>, <is_associative=True>)`` - The reduction function takes a
   arrow function and breaks the supplied collection (``TSL``/``TSD``) value into a
   set of tuples (filling in the zero when we have an odd pairing). This is then
   recursively applied until a single value is obtained.

