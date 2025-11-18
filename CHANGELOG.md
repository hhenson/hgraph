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

Version 0.4.0 (22-02-2025)
--------------------------

* Change the signature of switch_ to have key and then cases from switches and then key. This is a BREAKING change.


Version 0.4.1 (03-03-2025)
--------------------------

* Add combine operation for TSL keys TSL values -> TSD


Version 0.4.2 (04-03-2025)
--------------------------

* Add combine operator for TS[tuple] and TS[tuple] -> TSD
* Add values_ operator for TSD[K, TS[V]] -> TSS[V]

Version 0.4.3 (05-03-2025)
--------------------------

* Add lag for TSD proxy


Version 0.4.4 (14-03-2025)
--------------------------

* Add apply operator to support calling standard functions from a graph with time-series inputs

Version 0.4.5 (16-03-2025)
--------------------------

* Ensure args are valid before applying to function.

Version 0.4.6 (16-03-2025)
--------------------------

* Add proxy lag TSB 

Version 0.4.7 (17-03-2025)
--------------------------

* Bug fixes, rename hgraph tests to hgraph_unit_tests

Version 0.4.8 (17-03-2025)
--------------------------

* Change the Frame value parser to extract the schema from a value type.

Version 0.4.9 (18-03-2025)
--------------------------

* Fix issue with reference binding

Version 0.4.10 (18-03-2025)
---------------------------

* Handle deletes correctly when proxy lagging TSD's
* Add proxy lag for TSS
* Add proxy lag from TSL

Version 0.4.11 (18-03-2025)
---------------------------

* Fix a bug in TSS that computes the added property 

Version 0.4.12 (19-03-2025)
---------------------------

* Fix implementation of gate.

Version 0.4.13 (21-03-2025)
---------------------------

* Remove debug_print in lag operator

Version 0.4.14 (22-03-2025)
---------------------------

* Add dynamic TSB schema specification using TSB["a": TS[...], ...] syntax
* Add DebugContext to make it easier to leave debug statements in place but turn them off when not required (without runtime penalties).

Version 0.4.15 (22-03-2025)
---------------------------

* Add support for both combine(...) and combine[TSB](...) to be consistent with combine[TSD]
* Add ability to call **kwargs on TSB wiring port

Version 0.4.15 (24-03-2025)
---------------------------

* Make eq_ for TSD use reduce and map for implementation.

Version 0.4.17 (25-03-2025)
---------------------------

* add round_

Version 0.4.18 (28-03-2025)
---------------------------

* add filter_by operator and tsd implementation.
* add lower operator.

Version 0.4.19 (28-03-2025)
---------------------------

* Fix issue when the first argument to a lambda function in map_ is a _Marker class.


Version 0.4.20 (31-03-2025)
---------------------------

* Add TIME_TYPE type-var to indicate a date or a datetime.
* Add mean for TSW
* Fix a few issues with the TSW implementation
* Add constraint check on to_window
* Add std for TSW

Version 0.4.21 (01-04-2025)
---------------------------

* Update documentation
* Add hgrah.arrow module for supporting arrow style operators.

Version 0.4.22 (03-04-2025)
---------------------------

* Expand arrow API support for basic operators such as eq_, etc. and some control flow operators (if_.else.otherwise)
  and if_else.otherwise and fb (feedback). Clean-ups of testing and debugging tools. Add null support

Version 0.4.23 (04-04-2025)
---------------------------

* Add switch_, map_, reduce arrow operators


Version 0.4.24 (08-04-2025)
---------------------------

* Clean up Sphinx documentation and add adaptors.tornado Rest API docs.

Version 0.4.25 (10-04-2025)
---------------------------

* Update to_json to fix problem with tuples and nexted data structures.

Version 0.4.26 (10-04-2025)
---------------------------

* Fixes to the arrow wrappers, expose make_tuple to create a appropriate tuple wrappers.

Version 0.4.27 (10-04-2025)
---------------------------

* Change arrow tuples to be explicit named bundles to avoid confusion when auto-flattening the tuples.

Version 0.4.28 (11-04-2025)
---------------------------

* Add convert datetime->date
* Many small changes to arrow API
* Add flatten_tsl for converting Pair's to TSL
* Add flatten_tsb for converting Pair's to TSB
* Add support to bind TSB to node inputs
* Fix for mesh ranking

Version 0.4.29 (13-04-2025)
---------------------------

* Prepare WiringNodeClass to make use of C++ builders.

Version 0.4.30 (14-04-2025)
---------------------------

* Add support for arrow nodes which have side effects (such as debug_ and assert_)
* Small fixes when calling function in arrow

Version 0.4.31 (15-04-2025)
---------------------------

* Add hook to replace NodeSignature


Version 0.4.32 (15-04-2025)
---------------------------

* Add hooks to expose dependencies for creating the node-signature.

Version 0.4.33/4 (16-04-2025)
---------------------------

* Add support for subclass json serialisation / de-serialisation.
* Make injectables an integer value to support migration to C++ in the NodeSignature

Version 0.4.35 (17-04-2025)
---------------------------

* Implement from_dict for CompoundScalar


Version 0.4.36 (17-04-2025)
---------------------------

* Add substr
* fix converter function name (for converting datetime->date)
* Add the ability to include the discriminator field in the dataclass being serialised.
* Add serialisation of Set in to/from JSON
* Better None handling in to/from JSON

Version 0.4.37 (17-04-2025)
---------------------------

* Convert the project to make use of uv instead of poetry

Version 0.4.38 (20-04-2025)
---------------------------

* Prepare Edge for C++ support


Version 0.4.39 (25-04-2025)
---------------------------

* Add support for bytes data type.
* Add initial support for Kafka subscribers and publishers.

Version 0.4.40 (26-04-2025)
---------------------------

* Improve message API to support dynamically supplied topics, as well as only returning the 'out' value of a TSB
  output in the publisher to make the call graph cleaner.

Version 0.4.41 (27-04-2025)
---------------------------

* Correct generation of injectable inputs

Version 0.4.42 (26-04-2025)
--------------------------

* Add TRAIT to injectables enum and extraction logic.

Version 0.4.43 (28-04-2025)
---------------------------

* Add the ability to merge two compound scalar values.


Version 0.4.44 (29-04-2025)
---------------------------

* Add to_pair operator for arrow.
* Add add_ and sub_ for tuple and scalar value
* add sub_ with a comparator lambda function.

Version 0.4.45 (30-04-2025)
---------------------------

* add the ability to perform ``reduce`` on a tuple as well as non-associative TSD's where the TSD is integer indexed and the
  integers represent the range [0, n) where n is the size of the TSD.

Version 0.4.46 (01-05-2025)
---------------------------

* Fix __getitem__ on TSBRefWiringPort
* Fix wiring issues with arrow wrappers.

Version 0.4.47 (02-05-2025)
---------------------------

* Lots of small fixes
* Modify http logic to use bytes instead of str
* throttling TSS
* Collapsing messages
* Faster graph topological sorting
* Support for forward reference in schemas
* Initial support for threaded GlobalContext

Version 0.4.48 (06-05-2025)
---------------------------

* Small bug fixes
* Expose debugging options to eval_ in arrow api.

Version 0.4.49 (06-05-2025)
---------------------------

* Fix to kafka subscriber logic.
* Fix to_json decode when no value is present
* Add call (sink node version of apply)

Version 0.4.50 (06-05-2025)
---------------------------

* Clean-up logging to make it work more consistently.
* Modify merge_compound_scalars to be combine_compound_scalars overriding comnbine instead of merge.

Version 0.4.51 (06-05-2025)
---------------------------

* Add setattr_

Version 0.4.52 (07-05-2025)
---------------------------

* Remove a feature added by AI (bad import)


Version 0.4.53 (07-05-2025)
---------------------------

* Change setattr to make use of copy instead of to_dict

Version 0.4.54 (08-05-2025)
---------------------------

* add add_(TSS, TS) and sub_(TSS, TS)

Version 0.4.55 (13-05-2025)
---------------------------

* Bug fix to mesh
* fixes to documentation
* other small fixes

Version 0.4.56 (21-05-2025)
---------------------------

* Expose dereference operator

Version 0.4.57 (28-05-2025)
---------------------------

* Add breakpoint_ operator
* Add date + time, including support for time-zone handling.


Version 0.4.58 (28-05-2025)
---------------------------

* Add evaluation_time_in_range to support checking if a value is within a particular range of time.

Version 0.4.59 (28-05-2025)
---------------------------

* add initial support for from_data_frame

Version 0.4.60 (29-05-2025)
---------------------------

* Fix TSL Ref wiring port for iteration __getitem__

Version 0.4.61 (06-06-2025)
---------------------------

* Fix imports
* Fix conversion logic for tsd collect


Version 0.4.62 (10-06-2025)
---------------------------

* add ln operator
* start on a set of numpy wrappers and utilities to work with numpy objects

Version 0.4.63 (11-06-2025)
---------------------------

* Add from_data_frame_tsd_k_v overload
* Add to_data_frame for TS[...] and TSD[K, TS[...]]
* move recording to use . as the separator rather than - as this still causes some unpleasantness. 


Version 0.4.64 (17-06-2025)
---------------------------

* Add removed_value to the TSW data type.
* Update sum to make use of the removed_value and add mean window operator

Version 0.4.65 (27-06-2025)
---------------------------

* Add set_delta method to support factory-based instantiation of the set delta.
* Add type support to set_delta factory method and update code to make use of this.

Version 0.4.66 (30-06-2025)
---------------------------

* Clean up added / removed.

Version 0.4.67 (02-07-2025) 
---------------------------

* Clean up path

Version 0.4.68 (14-07-2025)
---------------------------

* redact Athorization cookie value

Version 0.4.69 (15-07-2025)
---------------------------

* Use json.dumps to convert str to json string in _to_json (to handle correctly escaping special characters)

Version 0.4.70 (31-07-2025)
---------------------------

* Clean up imports and documentation.

Version 0.4.71 (04-08-2025)
---------------------------

* Remove untested and broken convert of tuples to TSD
* Add test and fix combine of tuples to TSD

Version 0.4.72 (11-08-2025)
---------------------------

* Small fixes TSW

Version 0.4.73 (29-08-2025)
---------------------------

* Small fix to type description handling of underlying array in TSW.
* new run_on_thread adaptor
* Improvements to get-item with nested TSD
* Add a drop with a time-period constraint.

Version 0.4.74 (17-09-2025)
---------------------------

* Update mean over TSW (remove bad override and correct bugs in incremental implementation)
* Add reset to mean

Version 0.4.75 (23-09-2025)
---------------------------

* Add basic quantile support
  
Version 0.4.76 (23-09-2025)
---------------------------

* Add to_data_frame support for TSB.

Version 0.4.77 (23-09-2025)
---------------------------

* Add to_data_frame support for TSD[K, TSB]
* Add from_data_frame support for TSD[K, TSB]

Version 0.4.78 (24-09-2025)
---------------------------

* Add to_data_frame schema setting to ensure even values are missing the frames have the correct schema.

Version 0.4.79 (24-09-2025)
---------------------------

* Ensure we only sum valid values in the TSL scalars case.

Version 0.4.80 (03-10-2025)
---------------------------

* Add the ability to better describe the schema shape to produce for record/replay with data-frames.

Version 0.4.81 (07-10-2025)
---------------------------

* Fix missing imports

Version 0.4.82 (09-10-2025)
---------------------------

* Add compute_set_delta utility function.

Version 0.4.83 (09-10-2025)
---------------------------

* Add support for patching GlobalState with custom implementations.
* Add set_implementation_class and get_implementation_class methods to GlobalState.
* Override __new__ to support implementation class patching.

Version 0.4.84 (09-10-2025)
---------------------------

* Put GlobalState back to its original state.
* Reduce some timeouts on tests as they are starting to take too long.

Version 0.4.85 (15-10-2025)
---------------------------

* Cleanups to better support the C++ build.

Version 0.4.86 (15-10-2025)
---------------------------

* Add support for the C++ build.

Version 0.4.87 (16-10-2025)
---------------------------

* Expose key_tp to mesh builder to better support the C++ build.

Version 0.4.88 (17-10-2025)
---------------------------

* Some cleanups for the C++ build.
* Changes from AB mostly related to shut down handing, reference binding/unbinding and memory management.
* Some changes to perspective and the inspector.

Version 0.4.89 (17-10-2025)
---------------------------

* Add OutputKeyBuilder to better support the C++ build.
* Cleanups to better support the C++ build.

Version 0.4.90 (17-10-2025)
---------------------------

* Remove unnecessary use of internal TSD data structures.

Version 0.4.91 (17-10-2025)
---------------------------

* Expose key tp to the mesh builder.

Version 0.4.92 (18-10-2025)
---------------------------

* Remove inconsistent handling of subscriptions to inputs.
* Add feature to all returning a frozenset and having the TSS output conform to the set. THIS COULD BREAK EXISTING CODE.

Version 0.4.93 (22-10-2025)
---------------------------

* Add a fix for Non-peered TSD's where it is possible that newly created wrappers (in _create) will not be marked active
* A fix to signal where somehow the Python code was working with signal bound to non-peered TS instances where the added 
  children were not being marked as active.
* Remove the make active in the on_key_added as this is too far away from where the input is actually created and there
  may be other paths that could construct a non-peered TS instance. The logic is now with _create.
* Remove reverse mappings in TSD Input when removing a key
* Add is_reference to avoid instanceof check.

Version 0.4.94 (23-10-2025)
---------------------------

* Change _modified_items to dict from list to more closely match c++ implementation.

Version 0.4.95 (23-10-2025)
---------------------------

* Modify the _owning_node and _parent[_input|_output] to be _parent_or_node as in teh C++ code. This reduces the overall
  memory footprint of the graph, but will probably have a bit more cost to process in Python, but trying to keep C++
  and Python code as similar as possible to ellimate causes for bugs as I am porting. NOTE: THIS COULD BREAK EXISTING CODE.

Version 0.4.96 (23-10-2025)
---------------------------

* Only add a child to modified if the child's key exists in _ts_values, if not just return. NOTE: THIS COULD BREAK EXISTING CODE.

Version 0.4.97 (25-10-2025)
---------------------------

* Add calls to super for the release_instance method on the node builders
* Small improvements to reduce

Version 0.4.98 (27-10-2025)
---------------------------

* Fix dedup to not depend on runtime type information.

Version 0.4.99 (27-10-2025)
---------------------------

* Fix isinstance to TimeSeriesReference.is_instance on dereference node.

Version 0.4.100 (27-10-2025)
---------------------------

* Use set_delta to construct a new set_delta instance.
* Fixes to unit tests to better support the C++ build.

Version 0.4.101 (27-10-2025)
---------------------------

* Clean up the use of internal structure in compute nodes.

Version 0.4.102 (27-10-2025)
---------------------------

* Deal with non-peered TSD's view of contains method.

Version 0.5.4 (03-11-2025)
--------------------------

* Moved c++ code from hg_cpp into main hgraph package
* add a feature switch to control use of CPP engine (HGRAPH_USE_CPP=1)
* Cleanups to improve the stability of tests on CICD

Version 0.5.6 (09-11-2025)
--------------------------

* Add observer support and a few bug fixes.

Version 0.5.7 (10-11-2025)
--------------------------

* Refactoring cleanup to prepare for further C++ improvements.
* Performance optimisation for accessing evaluation_time.

Version 0.5.8 (18-11-2025)
--------------------------

* Convert SetDelta to be a value type and not a heap type.
* Convert TimeSereisReference to be a value type and not a heap type.
* Add visitor support to the TimeSeriesTypeInput/Output
* Some contributed fixes to make inspector work.
