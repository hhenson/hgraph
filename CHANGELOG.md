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
  
Version 0.2.31 (20-05-2024)
---------------------------

* Fix resolution order to a TSB[TS_SCHEMA] to fix to_frame resolution.

Version 0.2.32 (21-05-2024)
---------------------------

* Add min_ts and max_ts operator overloads
* Clean up location of ts operators to _ts_operators.py
* Expose ddof param in numpy std function
* Support partial parameter sets in TSB.from_ts defaulting missing values as nothing() nodes.

Version 0.2.33 (23-05-2024)
---------------------------

* Updates to improve TSD behaviour at scale.
* Many bug fixes.

Version 0.2.34 (30-05-2024)
---------------------------

* Update to introduce the operator decorator
* Modification of code to use operator for overloaded types.
* Support for *args and **kwargs in operator definitions
* Further updates and refinements to library design doc.
* Initial implementations for convert.
* Fix ref input modified (from Alex)
* Fix TSS diff where lhs is valid before rhs
* small error message fix

Version 0.2.35 (01-06-2024)
---------------------------

* Update handling of Generic schema's to properly manage the generic handling and resolution.
* Update parser to handle *arg: TSL[...] -> arg: TSL[...] (with mapping the elements to the time-series value)
* Similar change to handle **kwargs: TSB[TS_SCHEMA] -> kwargs: TSB[TS_SCHEMA] (with the provided kwargs being the 
  values making up the schema) (rough equivalent of TSB.from_ts(**kwargs) but automated.)

Version 0.2.36 (07-06-2024)
---------------------------

* Updates toward new standard library definitions

Version 0.2.37 (08-06-2024)
---------------------------

* Added all the operator overloads for all types where they did not exist in the required packages,
  and added tests for each combination of type and operator.
* Renamed all the overloads according to the document
* Added ISSUES.md until we can add to GitHub issues
* More converter and combines support for *args in signatures.
* Wiring Tracer support
* Updates to library rules.

Breaking changes:
* Moved sum_ into operators as per doc
* _ts_operators no longer exists, as per doc. Operators in it moved to the correct packages
* Removed (most) overloaded specific operator names from all
* and_op renamed to bit_and (and analogies for or_op etc) as per doc
* union_op removed (is equivalent to bit_or)
* match renamed match_ (match is now a python keyword)
* modified_datetime/date removed in favour of the version: hgraph.nodes._time_series_properties.last_modified_time/date
* Removed UnSet

Version 0.2.38 (08-06-2024)
---------------------------

* Fix removal of tsl_get_item_ts

Version 0.2.39 (08-06-2024)
---------------------------

* Make scalar operators use actual operators rather than the dunder methods

Version 0.2.40 (12-06-2024)
---------------------------

* Improvements to ranking of TSL
* Remove brute force conversion of time-series-schema parameters
* Add more conversion and emit operator implementations
* Corrections to docs

Version 0.2.41 (13-06-2024)
---------------------------

* Cleanups on min, max, sum


Version 0.2.42 (15-06-2024)
---------------------------

* Standardised flow control operators (if_, merge etc)
* Added stream_analytical operators. 
* string library 
* final batch of converts, combines, collects and emits. small bug fixe… 
* Add operators for tsds and mappings with implementations for tsd and …
* Implement stream operators
* Change resample API
* Add __end_time__ to resample and throttle tests
* fix schema base generic test 
* fix getitem for fixed tuples 
* fix reduce on the TSDWiringPort and add filter to the tracer

Version 0.2.43 (17-06-2024)
---------------------------

* Clean up operators

Version 0.2.44 (18-06-2024)
---------------------------

* Move remaining operators to _operators, clean up tests, etc.

Version 0.2.45 (20-06-2024)
---------------------------

* Re-work ranking logic in preparation for delayed binding

Version 0.2.46 (21-06-2024)
---------------------------

* Add delayed_binding

Version 0.2.47 (27-06-2024)
---------------------------

* Added cycle check

Version 0.2.48 (28-06-2024)
---------------------------

* Further cleanups of node locations and removing items that are no longer used.

Version 0.2.49 (04-07-2024)
---------------------------

* Upgrade to Polars 1.0, fixes due to API changes

Version 0.2.50 (15-07-2024)
---------------------------

* Ranking bug fixes
* Updates to perspective adapter
* First pass adapter implementation
* First pass wrapper for component implementation (still in alpha)

Version 0.2.51 (17-07-2024)
---------------------------

* Updates to the adaptor framework
* Small fixes to examples
* combine for date

Version 0.2.52 (26-07-2024)
---------------------------

* collection for TSD -> TSD and TSS -> TSS
* Some bug fixes
* Preparation of record / replay operators
* Work on adaptors

Version 0.2.53 (12-08-2024)
---------------------------

* Fixed imports

Version 0.2.54 (15-08-2024)
---------------------------

* Fix dataframe iteration when the iterator returns an empty set.
* Introduce RECOVER flag
* Fix TSD's ticking when not valid and {} is ticked.
* Implement initial recovery logic.

Version 0.3.0 (17-08-2024)
--------------------------

* Update roadmap to address re-order of work.
* Update to adaptors and adaptor logic
* Bug fixes

Version 0.3.1 (21-08-2024)
--------------------------

* Small fixes
* Initial cut of to_table and related code

Version 0.3.2 (21-08-2024)
--------------------------

* Fix to cycle detection logic

Version 0.3.3 (23-08-2024)
--------------------------

* Add const_fn decorator for wrapping a constant value function
  This allows for operator support and can be used outside the wiring context.
* Convert table_schema to const_fn
* Prepare replay logic from dataframes.

Version 0.3.4 (24-08-2024)
--------------------------

* Update to make use of poetry for project management.

Version 0.3.5 (27-08-2024)
--------------------------

* First pass of dataframe recording complete.
* Update documentation.

Version 0.3.6 (27-08-2024)
--------------------------

* Add to_json and from_json operators.

Version 0.3.7 (28-08-2024)
--------------------------

* Fix HttpAdaptor handling of TSB return values.
* Ensure the handler request is removed once the response is returned.

Version 0.3.8 (28-08-2024)
--------------------------

* More fixes to Tornado adaptor

Version 0.3.9 (29-08-2024)
--------------------------

* Fix to the value property on the WiringPort

Version 0.3.10 (02-09-2024)
---------------------------

* Add helper functions to extract service inputs and outputs.
* Add new logic to handle to_table and related methods. Provides a more generic solution to the implementation.
* Support TSD, TS, TSB and CompoundScalar serialisation.

Version 0.3.11 (03-09-2024)
---------------------------

* Fix to_table for nested TSD structures.

Version 0.3.12 (04-09-2024)
---------------------------

* Add modified operator.
* Fix imports
* Start on to_json serialisation

Version 0.3.13 (05-09-2024)
---------------------------

* Changes to the real-time scheduling
* Many bug fixes
* Work on perspective API to improve performance and design
* Graph inspector UI (web adaptor)
* Work on scheduler issues

Version 0.3.14 (05-09-2024)
---------------------------

* Add mapping and tuple support to ``to_json`` operator.

Version 0.3.15 (05-09-2024)
---------------------------

* Add from_json logic

Version 0.3.16 (10-09-2024)
---------------------------

* Bug fixes
* Work on graph debugging tools
* Preparation for docs to convert to sphinx and support for readthedocs

Version 0.3.17 (19-09-2024)
---------------------------

* Add rest_handler support

Version 0.3.18 (20-09-2024)
---------------------------

* Fixes to rest_handler and more tests

Version 0.3.19 (20-09-2024)
---------------------------

* More fixes to rest and http logic

Version 0.3.20 (24-09-2024)
---------------------------

* Clean up http adaptor handler logic and documentation.
* Add a single registration function for the http adaptor.
* Add rest client support

Version 0.3.21 (14-10-2024)
---------------------------

* Bug fixes
* Documentation updates

Version 0.3.22 (26-10-2024)
---------------------------

* Many bugfixes from AB/SY/TO including; issues with map_ inside a reduce, signal, tsd issues, operators, schema.
* Fix to reference service interface signature.

Version 0.3.23 (27-10-2024)
---------------------------

* Support nodes being wrapped by service_impl instead of having to create a stub graph to call them.
* Correct implementations of merge
* Fix bug in tsd_get_items when the input tsd goes away.

Version 0.3.24
--------------

* Add CICD tooling to auto-increment the version in pyproject.toml

Version 0.3.25 (30-10-2024)
---------------------------

* Fix issue with replay in replay record.

Version 0.3.26 (31-10-2024)
---------------------------

* Provide abstraction for a DataFrameStorage facility. Use this to support data frame reading and writing in the
  record-replay infrastructure.

Version 0.3.27 (2-11-2024)
--------------------------

* Initial recovery of recordable state. Limited to cases where the recordable_id can be provided.

Version 0.3.28 (3-11-2024)
--------------------------

* Add the ability to plot a time-series in a notebook.

Version 0.3.30 (3-11-2024)
--------------------------

* Reduce logging when using eval and plot

Version 0.3.31 (7-11-2024)
--------------------------

* add cpm_ and if_cmp operators
* Spelling fixes
* additions to plotting

Version 0.3.32 (16-11-2024)
---------------------------

* add initial support for nested graph state recovery.

Version 0.3.33 (20-11-2024)
---------------------------

* BUFF time-series type.

Version 0.3.34 (22-11-2024)
---------------------------

* buffered_window provides the window function logic using the new BUFF time-series type.

Version 0.3.35 (25-11-2024)
---------------------------

* Rename BUFF to TSW (TimeSeriesWindow) after discussion with AB.

Version 0.3.36 (6-12-2024)
--------------------------

* Bug fixes to TSD
* Additional date operators
* Work on perspective adaptors
* Work on monitoring
* Work on recovery logic
* Renaming of TSW implementation code

Version 0.3.37 (6-12-2024)
--------------------------

* Fix broken test
* Update poetry.lock

Version 0.3.38 (11-12-2024)
---------------------------

* add passive marker

Version 0.3.39 (12-12-2024)
---------------------------

* Update the perspective adaptor to support the latest version of the library.

Version 0.3.40 (23-12-2024)
---------------------------

* clean-up to build script
* update lock file.

Version 0.3.41 (10-01-2025)
---------------------------

* Add support for the new perspective adaptor.
* Improve merge support
* Mark web test as flaky as it tends to fail on CI.

Version 0.3.42 (14-01-2025)
---------------------------

* Clean up the TimeSeriesReference implementation to mirror the new C++ implementation.

Version 0.3.43 (17-01-2025)
---------------------------

* Add stream operator

Version 0.3.44 (23-01-2025)
---------------------------

* Add date operators and make count resettable

Version 0.3.45 (23-01-2025)
---------------------------

* Make index_of more generic

Version 0.3.46 (26-01-2025)
---------------------------

* add operator to support TSL op TS

Version 0.3.47 (27-01-2025)
---------------------------

* Fix problem with tss contains operator.

Version 0.3.48 (28-01-2025)
---------------------------

* Fix a few issues

Version 0.3.49 (29-01-2025)
---------------------------

* Cleanups to support replaying a data frame directly.

Version 0.3.50 (30-01-2025)
---------------------------

* add dependency to typing_extensions

Version 0.3.51 (03-02-2025)
---------------------------

* Add lift operator. This allows for the re-use of scalar functions as compute nodes.

Version 0.3.52 (03-02-2025)
---------------------------

* Add a ZERO return option for divide by zero

Version 0.3.53 (04-02-2025)
---------------------------

* Add dedup_output to lift operator
* Rename pass_through in nodes to pass_through_node to avoid confusion with the marker pass_through for map.

Version 0.3.54 (06-02-2025)
---------------------------

* Add divide by zero returns 1.0

Version 0.3.55 (06-02-2025)
---------------------------

* Add engine time to log and add ability to perform sampled logs.

Version 0.3.56 (11-02-2025)
---------------------------

* Fix eq_tsss implementation
* Add contains_tss_tss to support a set being contained in a set.
* Add sum_ and abs_ for TSW types

Version 0.3.57 (16-02-2025)
---------------------------

* Add support to help with bidirectional matches with HgCompoundScalarType
* Add more information to failed overload resolution

Version 0.3.58 (19-02-2025)
---------------------------

* Add lag support for proxy lagging, where a value can be lagged based on the ticks of another time-series.

Version 0.3.59 (20-02-2025)
---------------------------

* Clean up getting recordable id's using time-series value in the format string.

Version 0.3.60 (22-02-2025)
---------------------------

* Large number of updates to web interface
* Beginning of logic to merge adaptors and service infrastructure
* Bug fixes
