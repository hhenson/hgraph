# hgraph issues

1. __eq__ is defined on WiringNodeInstance.  This causes eq_tsds to fail because the correct __eq__ is not found on the overload. See test_tsd_operators.test_eq_tsds
2. Empty TSDs do no tick (at least in tests). Empty TSSs appear OK.  See test_tsd_operators.test_sum_tsd_unary
3. Invalid compute node inputs sometimes have UnSet as value and sometimes have None. See hgraph.nodes._tss_operators.min_tss_unary (UnSet) and hgraph.nodes._tuple_operators.min_tuple_unary (None).  None would be much more convenient. 