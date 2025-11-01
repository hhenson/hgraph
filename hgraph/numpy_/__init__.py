"""
Numpy Bindings
==============

Provides commonly used numpy methods lifted into HGraph.

These are named as per the numpy library to make it easy to port from numpy directly to HGraph.

.. note:: In HGraph, numpy arrays are represented using the ``Array`` type. This allows for describing the
        expected shape of the array.

Array
-----

The type accepts as the first generic, the value of the array, the next args are Size types representing the
dimensionality of the array.

For example::

    Array[float, Size[2], Size[2]]

This represents a numpy array with data type float, with a dimension of 2 by 2.

It is possible to not specify an unbounded size which implies the array in that dimension is not fixed in size.

There are two keys methods for constructing an array, one is as a const::

    a = const(np.ndarray(...), Array[...])

The other is to create a rolling window::

    a = to_window(ts, size, min_size)

The second approach will create a rolling window with a size of ``size`` and a minimum size of ``min_size``.
The actual type of the output in the second case is ``TSW`` or time-series window. The TSW has as value a numpy array.

.. note:: The TSW is valid once a tick has been received, the ``delta_value`` is equivalent to the input ts-series value.
          The ``value`` is a numpy array, if ``min_size`` is set, then the value is ``None`` until the min-size is
          achieved. To only get ticks when the min-size is achieved, use the all_valid constraint on the input.

To use the window as a ticking value of array, use the ``as_array`` to convert from ``TSW`` to numpy array.
"""

from hgraph.numpy_._constants import *
from hgraph.numpy_._converters import *
from hgraph.numpy_._mathematical_functions import *
from hgraph.numpy_._statistics import *
