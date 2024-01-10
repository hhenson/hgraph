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

