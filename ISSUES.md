# hgraph issues

1. [ ] __eq__ is defined on WiringNodeInstance.  This causes eq_tsds to fail because the correct __eq__ is not found on the overload. See test_tsd_operators.test_eq_tsds
2. [ ] Empty TSSs do not tick if they are initially empty (although they do when going empty)
3. [x] Would be good if switch_() had the keys arg first, to make the order natural as per other languages
4. [x] Need a substr() operator
7. [ ] dispatch/map_/switch_ implementation graphs fail if they have CONTEXT arguments. A workaround is to wrap the target graph in another without the CONTEXT.
9. [ ] Add a function which silently discards errors for a node (e.g. try_(node) or discard_errors(node)). This would avoid the need to capture the exception_time_series and wrap it in a null_sink
10. [-] log_() should add the engine time and also output to stdout if severity info. Currently logs to stderr for everything. [The second part of this is a logger config issue not a really a framework issue]
11. [ ] Add Union types to graph signatures so that (for example) dispatch branches don't have to be repeated for different subclasses
12. [ ] Implement per-item throttle (perhaps as an arg to throttle) so that individual elements in TSBs, TSDs and TSSs are throttled independently
13. [ ] Improve error handling for '.' operator on TS[CompoundScalar] if the attribute does not exist.  Currently a long list of overloads is quoted and the actual field in error is not reported. Similarly for combine().
14. [ ] It is currently not possible to have a map_ over a graph/node which has default values in it, unless the lambda form is used
15. [ ] convert() from date to datetime picks the wrong operator (should be convert_date_to_datetime but uses convert_ts_scalar_downcast). Also it is not timezone aware. Likewise datetime to date does not work.
16. [ ] registering services should only include type-resolution information for inputs that are part of the interface signature and not all inputs.
17. [x] The construction of keys to store and retrieve outputs in the GlobalContext should be extracted into a dedicated function along with a factory to fetch the function. This will help with the C++ implementation.
