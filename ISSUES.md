# hgraph issues

1. __eq__ is defined on WiringNodeInstance.  This causes eq_tsds to fail because the correct __eq__ is not found on the overload. See test_tsd_operators.test_eq_tsds
2. Empty TSDs do no tick (at least in tests). Empty TSSs appear OK.  See test_tsd_operators.test_sum_tsd_unary
3. Would be good if switch_() had the keys arg first, to make the order natural as per other languages
4. Need a substr() operator
5. Need a 'copy_with()' for CompoundScalar which handles expressions.  dataclasses.replace() fails with frozen dataclasses containing expressions currently
6. Add a 'replace()' operator implemented for bundles, TSD, TS[CompoundScalar] etc
7. dispatch implementation graphs fail if they have CONTEXT arguments