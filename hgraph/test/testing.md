Testing Library
===============

The hd.test package contains a library of functions to facilitate effective
unit testing and integration testing of hg graphs and nodes.

Unit Testing
------------

The most fundamental testing is unit testing of nodes. The hg.test package
provides a number of tools to ease testing nodes and to fuzz test nodes.

Unit testing is designed to test the functionality of a single node or a very
small graph of nodes. These tests are only ever run in back test mode and are
not designed for real-time testing.

The bulk of all hg testing should be preformed at this level. The
design philosophy of hg is to construct graphs (business logic) from
a set of well-defined and tested nodes.
