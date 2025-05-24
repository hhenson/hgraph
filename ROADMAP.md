Roadmap
=======

This is a list of features / tasks that are planned for future releases and
an idea of time-lines. The exact order of delivery may change as interest dictates.

## 0.1.0

This represents the first usable release of the library.

The following features should be working:
* Basic time-series data types (TS, TSB, TSD, TSL, TSS)
* Ability to define nodes and graphs.
* Ability to wire nodes together.
* Detection of incorrect wiring and type checking.
* Support for use of standard Python operators (+ - * / etc.) on time-series types.
* Automatic conversion of python values to constant time-series when required.
* Support for map and reduce operators to allow for processing multiplexed time-series.
* Support switch operator.
* Real-time and simulation modes of operation.
* Ability to define Python-based push (realtime) and pull( realtime/simulation) source nodes.
* Feedback operator.
* Basic documentation to describe the concepts and usage of the core functionality of the library.

Note this makes the library functionally usable, albeit lacking in more advanced features
required to build more complicated scenarios. The intention is that it would be possible
to implement basic graphs such as signal generation that will support real-time and
simulation modes of operation.

## 0.2.0

The following features are planned for this release:
* services - ability to define time-series services that can be used to construct larger asynchronous graphs.
* service discovery - ability to dynamically discover services and wire them together.
* reference data services - A service that provides access to data with no request for the data.

Note the first version of services are implemented in process and are not distributed.

## 0.3.0

The following features are planned for this release:
* Graph recording and playback.

## 0.4.0

The following features are planned for this release:
* distributed network services. [For now move this to a post 1.0 feature]
* External API support (REST, gRPC, etc.). Allows for exposure of the graph to non-graph users. [This is in place as adaptors to various external protocols]

## 0.5.0

The following features are planned for this release:
* Develop the core standard library. This should hopefull be a review of the components
  developed to date and ensuring naming consistency, validate documentation, etc.
* Bring documentation in line with the core library and ensure it is up-to-date in preparation
  for the 1.0.0 release.

## 1.0.0

This release contains all features listed prior to this, with agreed and finalised
core requirements to be a valid hgraph implementation.
This release should have a feature complete python implementation of the engine 
and associated run-time requirements, but be sufficiently decoupled as to allow
the development of alternative language implementation of the runtime components.

## 1.1.0

The first release of the C++ implementation of the runtime engine.
