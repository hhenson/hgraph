# AnyValue

This is a type-erased value holder. It provides a generic value holder with generic
behaviour exposed via the `Value` interface.

The most important behaviours are:

* equality
* hashing
* to_string

The equality and hashing are to support being used as keys in maps or values in sets.

The to_string is to support being used in logging and debugging.

The any-value can also take a reference to a value. This allows for no-copy values, then if the value is moved or copied, 
it will make a full copy of the value. It is also possible to force materialization of the value.

## Functions:

std::hash\<AnyValue\> - To support stl containers.

to_string\<AnyValue\> - To support logging.

== - Equality operator, implemented as friend operator with equal VTABLE entry.

