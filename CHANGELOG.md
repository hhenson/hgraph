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
