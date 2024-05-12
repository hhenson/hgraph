Analytics Library
=================

Provide a collection of nodes and utilities that can be used to perform incremental computations
and analytics. The focus will be to create stateless elements, where any state that is 
required is emitted as time-series values using recordable feedback nodes to return the values.

This will allow values to be recorded and appropriately reloaded. This is a lighter weight version
of creating a generic graph recording capability.

Key components include:

* RecordableFeedback

