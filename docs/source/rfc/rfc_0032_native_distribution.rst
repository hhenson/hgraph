RFC 0032: Native Distribution of the SDK
========================================

:Status: Revised for language extraction; language channel proposals moved
:Author: Howard Henson
:Created: 2026-09-04
:Target: native install tree, Conan recipe, release process

Runtime distribution
--------------------

The hgraph package owns the runtime SDK, public headers, native libraries and
``hgraphConfig.cmake``. The optional Python bridge ships through the existing
wheel channel. Native consumers use ``find_package(hgraph CONFIG REQUIRED)``
and pin the matching SDK release; the native API remains source-provisional.

The Conan recipe packages the runtime SDK without compiler tools. Its installed
consumer exercises the native wiring API. Runtime builds, source archives and
release jobs do not build or distribute the HGL compiler or its dependencies.
The release-version and compatibility policies remain in
:doc:`../developer_guide/release_readiness`.

Language distribution
---------------------

The compiler, native-module tooling and language distribution preparation now
belong to the separate public
`hgraph_lang project <https://github.com/hhenson/hgraph_lang>`_. That project
consumes the installed runtime SDK and owns its own build, tests and release
version. The runtime has no dependency on the language package.

The former combined-distribution proposal is retained in
`the extracted design record <https://github.com/hhenson/hgraph_lang/blob/main/language/docs/design/native-distribution-before-extraction.rst>`_.
Its release-switch steps are historical proposals, not runtime release steps.
No additional package channel is published by this extraction.
