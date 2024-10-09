Typing
======

The typing system is managed using a meta data wrapper for the Python types along with the appropriate extraction
logic to parse a signature to extract the types. The entry point into the type meta-data system is:

.. autoclass:: hgraph.HgTypeMetaData
    :members:
    :undoc-members:


The support for schema based types has as it's base:

.. autoclass:: hgraph.AbstractSchema
    :members:
    :undoc-members:

For scalar values, use:

.. autoclass:: hgraph.CompoundScalar
    :members:
    :undoc-members:

For time-series schema to be used with ``TSD`` use this as a base:

.. autoclass:: hgraph.TimeSeriesSchema
    :members:
    :undoc-members:


To assist with dynamic schema construction the following are provided:

.. autofunction:: hgraph.compound_scalar

and for time-series schemas:

.. autofunction:: hgraph.ts_schema

