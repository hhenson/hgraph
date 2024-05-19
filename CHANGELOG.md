Changelog
=========

A brief summary of the changes made.

Version 0.0.4 (23-12-2023)
--------------------------

 * **map_** - completed support for TSD and TSL (with fixed size). Support for variable length TSL will be implemented
          once TSL with variable sized inputs are fully supported.
 * **reduce** - Implement TSD and TSL (with fixed size). At the moment only TSL implementations support non-associative
                type implementations.

Version 0.0.5 (26-12-2023)
--------------------------

 * Bug fixes to map_.
 * Fixes to node conflation.
 * Fixes to REF handling in TSD.
 * Documentation updates. Most of the basic documentation is now in place.

Version 0.0.6 (27-12-2023)
--------------------------

* Add feedback operator.

Version 0.0.7 (02-01-2024)
--------------------------

* Add documentation describing PUSH Queues.
* Add first pass support for error handling
* Add a few small node extensions (e.g. emwa)

Version 0.0.8 (05-01-2024)
--------------------------

* Add try_except and exception_time_series to deal with exception management in graph.

Version 0.0.9 (08-01-2024)
--------------------------

* Add __getitem__ for TSD to get the value as a time-series from a key as a time-series.
  Support ``ts = my_tsd[key_ts]`` in a graph context.
* Extract extension logic, make contains_ work for TSS and TSD.
* Implement not_ and is_empty for TSS and TSD

Version 0.0.10 (08-01-2024)
---------------------------

* Fix an issue with AUTO_RESOLVE
* Fix missing defaults in window implementations
* Fix imports and __all__ statements in nodes
* Preparation work for NodeInstance inlining
* Add mean and len_ operators
* Add all_valid attribute to decorators, WiringNodeSignature and NodeSignature as well as impl.

Version 0.0.11 (10-01-2024)
---------------------------

* Add min_window to window node.
* Fix resolve_all_valid_inputs
* Ensure that we check for start and end times not exceeding bounds
* Add take, drop
* Add lt_ operator, and default implementations of le_ and or_
* Improve error when an operator overload fails.
* add min_window_period to rolling_average
* Fix a bug in reduce over a TSL
* Fix average for float values.
* Default implementation of abs_
* add clip analytical function


Version 0.1.0 (10-01-2024)
--------------------------

* Add format_ to perform time-series formatting of a string.
* replace write_... with print_ to keep naming more consistent with std python.

NOTE: This release is to indicate a milestone. The code meets
the objectives of the 0.1 release. This will still have a lot
of work cleaning up and adding to the standard library.


Version 0.1.1 (13-01-2024)
--------------------------

* Fix missing try_except example
* Add len support to TSL wiring port
* Error check on returning a TS Schema instead of TSB\[Ts Schema]
* Add an example of using the graph with a Flask application.
* Start describing the service infrastructure.
* Remove dependnecy on more-itertools (replace nth and take functions)


Version 0.1.2 (14-01-2024)
--------------------------

* Prepare for reference services
* Add Enum support to scalars.


Version 0.1.3 (15-01-2024)
--------------------------

* Perform interning of WiringNodeInstance
* De-dup processing nodes that have already been processed.
* Add values, items, and keys to TSLWiringPort
* Add support to expand generator in from_ts on TSL.
* Make lag generic and add tsl_lag (lag support for TSL's)


Version 0.1.4 (15-01-2024)
--------------------------

* Fix for equality check of inputs in WiringNodeInstance interning


Version 0.1.5 (21-01-2024)
--------------------------

* Make end-time exclusive
* Convert TSD and TSS keys to KEYABLE_SCALAR
* Add an extension for python object (HgObjectType) this will match any non-time-series value.
* ensure lt_ returns a bool value, and add default behaviour for gt_ and ge_.
* Add sum_
* Add support for ndarray (as Array)
* Rename window to rolling_window
* Add np_rolling_window to get first experimental numpy API tested

Version 0.1.6 (22-01-2024)
--------------------------

* Add tsl_to_tsd
* Adjust min-version of numpy requirement
* Fix bug in window logic

Version 0.1.7 (24-01-2024)
--------------------------

* Update docs
* Fix __trace__ to display _schedule when present and mark when active.
* Fix bug in scheduler


Version 0.1.8 (25-01-2024)
--------------------------

* Fix bug with matches on compound scalar.
* Fix bug with processing compute_nodes with injectable attributes within a map.


Version 0.1.9 (01-02-2024)
-------------------

* Add support for overrides where inputs are constants and not time-series values (where the
  original input was a time-series value)
* Initial implementation of reference data services complete

Version 0.1.10 (05-02-2024)
---------------------------

* Fix a scheduler bug when updating a scheduled node in a map. Hard to replicate, the test
  does not replicate.

Version 0.1.11 (07-02-2024)
---------------------------

* Fix issue with scheduler
* Add __trace__ to run_graph command
* Add additional details around scheduling to trace (specifically detailing when scheduling has occurred
  and scheduling times for nodes).


Version 0.1.12 (12-02-2024)
---------------------------

* Implement subscription service

Version 0.1.13 (16-02-2024)
---------------------------

* Fix use of graph's in switch


Version 0.1.14 (19-02-2024)
---------------------------

* Fix switch use of key
* Fix switch stop when the switch itself goes away

Version 0.1.15 (21-02-2024)
---------------------------

* Fix issue with nested graphs not ticking correctly.


Version 0.1.16 (21-02-2024)
---------------------------

* Fix accidental issue added during fixing of nested graph issue.

Version 0.1.17 (02-03-2024)
---------------------------

* Attempt to ensure we get a stable flattening of the topologically sorted graph.
* Clean up const to use start_time as an anchor
* Fix nested graph to use the evaluation time as start time, this fixes using const in a nested graph.
* Update eval_node to support life-cycle observers to be added
* Update README.md
* Update to start making using of Python logging for the engine.
* Extract runtime configuration into a standalone object.

Version 0.1.18 (13-03-2024)
---------------------------

* Multiple small fixes.

Version 0.1.19 (15-03-2024)
---------------------------

* Many fixes to deal with REF data structures.
* Implementation of Request Reply Services.
* Many bug fixes.
* Move where inner graphs wiring to appropriate locations to ensure they are processed during
  wiring time.
* Separate parsing into type and value parsing.
* Add getitem_ and getattr_ operators
* Support auto zero in reduce
* Add support for lambda's in reduce and map_

Version 0.2.0 (15-03-2024)
--------------------------

* No changes, just a release as we have reached the objectives of the roadmap for 0.2

Version 0.2.1 (16-03-2024)
--------------------------

* Fix tsd_rekey
* Fix tsd_flip
* Add tsd_flip_tsd
* Fixes to tsd_collapse_keys and tsd_uncollapse_keys

Version 0.2.2 (21-03-2024)
--------------------------

* Many bug fixes around REF type.
* New nodes around TSD and Frame manipulation
* Some changes in preparation for graph recording.

Version 0.2.3 (22-03-2024)
--------------------------

* Extend resolvers to all node decorators
* Fix np_percentile signature
* Cleanup operators

Version 0.2.4 (24-03-2024)
--------------------------

* add pct_change
* add np_std
* A few more preparatory changes for graph recording

Version 0.2.5 (25-03-2024)
--------------------------

* Fix min_of_two (from graph to compute_node)

Version 0.2.6 (26-03-2024)
--------------------------

* Fix timing issue with tsd_rekey
* Clean up imports and __all__ in nodes

Version 0.2.7 (06-04-2024)
--------------------------

* Add support for multi-service implementations
* Support multiple inputs in a request-reply service
* nested tsd merge
* constraint type wiring improvements

Version 0.2.8 (10-04-2024)
--------------------------

* Add initial notebook support
* Add perspective adaptor
* Add support for generic services

Version 0.2.9 (14-04-2024)
--------------------------

* Add ability to mark nodes as being deprecated.


Version 0.2.10, 11
------------------

* Experimental attempt to build with feature support


Version 0.2.12 (19-04-2024)
---------------------------

* Add Logger injectable
* Clean up schema accessor methods
* Add ability to move between TSB and Scalar structures
* Add filter_ node

Version 0.2.13 (20-04-2024)
---------------------------

* Add support to publish directly from GitHub.

Version 0.2.14 (24-04-2024)
---------------------------

* Add context support
* Add support for casting to base-class when wiring.


Version 0.2.15 (27-04-2024)
---------------------------

* Add unroll node
* Clean up feedback signature
* Clean up inputs

Version 0.2.16 (29-04-2024)
---------------------------

* Add copy_with to the TSBWiringPort

Version 0.2.17 (29-04-2024)
---------------------------

* Add profiling support
* Updates to context support

Version 0.2.18 (01-05-2024)
---------------------------

* Don't try and invalidate values unless it is set explicitly (i.e. not as a property of a dict supplied ot a bundle)
* Catch duplicate time in generator code to avoid misleading exceptions when this happens.

Version 0.2.19 (03-05-2024)
---------------------------

* Clean up processing of data frames.
* Change and improvements to CONTEXT support
* Additions to documentation

Version 0.2.20 (03-05-2024)
---------------------------

* Add ability to construct un-named compound scalar types
* Add data frame iteration support.


Version 0.2.21 (04-05-2024)
---------------------------

* Add date and datetime operators
* Clean up generic operators to force all template types (defined in hgraph) to be un-implemented
* Implement the core operators that have unit tests in this library.


Version 0.2.22 (07-05-2024)
---------------------------

* Add generic set operators in hgraph and adjust implementors in nodes package.
* Experimental approach to frame operators.
* Fixes to services where impl has more type resolutions than the interfaces.

Version 0.2.23 (11-05-2024)
---------------------------

* Make generics work in dispatch
* Add max_ operator
* Fixes to docs and tests

Version 0.2.24 (14-05-2024)
---------------------------

* Fix eq_ts to force cast to bool result
* Add or_ts and and_ts
* Some initial structure for recorded analytics

Version 0.2.25 (14-05-2024)
---------------------------

* GlobalState __enter__ must return self
* Fix pre-resolved node wrapper start / stop properties to delegate to underlyer

Version 0.2.26 (15-05-2024)
---------------------------

* Initial implementation of mesh_ computation fabric.
* Fixes to switch_ and map_

Version 0.2.27 (15-05-2024)
---------------------------

* Small fix of eval_node, move GlobalState to a higher level in the wrapper


Version 0.2.28 (16-05-2024)
---------------------------

* Add mod_ts to implement the mod_ operator over scalars.
* Add a sample param to format_ to allow for easy construction of sampled text for debugging.
* Add a sample param to debug_print

Version 0.2.29 (17-05-2024)
---------------------------

* Bug fixes
* drop_dups specialisation for float
* log node
* tss operators
* TSL improvements
* allow switch graphs to have no output

Version 0.2.30 (19-05-2024)
---------------------------

* Add explicit mappings for <=, >=, >, !=
* Make compound scalar objects match UnNamedCompoundScalar objects with the same properties.
* add extraction of a single ts value from a data-frame source
* to_frame operator with:
  * to_frame_ts
  * to_frame_tsb
  
