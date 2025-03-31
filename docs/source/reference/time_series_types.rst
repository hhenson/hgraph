Time-Series Types
=================

The time-series types are the key types in the HGraph library as they provide the ports to support connecting
outputs and inputs together forming edges in the graph. It is important to note that the API's associated to the
aliased types are contractual and may not be the instance types provided when actually instantiated by the runtime.
This is especially true when using an alternative runtime (such as a C++ engine). In other words do not write code
that does ``isinstance`` or ``issubclass`` of these types in nodes.

The supported time-series types are:

.. autoclass:: hgraph.TS
    :members:

.. autoclass:: hgraph.TSS
    :members:

.. autoclass:: hgraph.TSL
    :members:

.. autoclass:: hgraph.TSB
    :members:

.. autoclass:: hgraph.TSD
    :members:

There is a special type to wrap any time-series type (as an input):

.. autoclass:: hgraph.SIGNAL
    :members:

Then there is a reference type, which can point to any of the above standard time-series types:

.. autoclass:: hgraph.REF
    :members:

Finally, there is a special buffer type that is used to define a buffered time-series.

.. autoclass:: hgraph.TSW
    :members:

.. note:: The above types are alias types, they are used when annotating the types that a function supports as inputs
          any may return as an output. The actual types are the inputs when using them as inputs to a function and
          are outputs when used as a return value, or when used as the injectable ``_output``. There are corresponding
          ``_OUT`` aliases that can be used to type the ``_output`` for better IDE support.

The bases of all time-series types are:

.. autoclass:: hgraph.TimeSeries
    :members:
    :undoc-members:

.. autoclass:: hgraph.TimeSeriesOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesInput
    :members:
    :undoc-members:
    :show-inheritance:

The detailed API's of the remaining types are presented below:

.. autoclass:: hgraph.TimeSeriesDeltaValue
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesSignalInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesValueOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesValueInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.SetDelta
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesSet
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesSetInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesSetOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesIterable
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesList
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesListInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesListOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesBundle
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesBundleInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesBundleOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesDict
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesDictInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesDictOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesReference
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesReferenceOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesReferenceInput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesWindow
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesWindowOutput
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.TimeSeriesWindowInput
    :members:
    :undoc-members:
    :show-inheritance: