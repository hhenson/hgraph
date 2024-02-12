HG-2024-02-10-Suspend-Resume
============================

Support the ability to suspend the operation of the graph and then resume it's
operations later.

The feature allows for graphs that are long-running, but only spends short periods
performing actual work. In this case, we can choose to suspend the graph once the
key computation is complete and resume it when it is required to compute inputs
once again.

The upside is we can have multiple graphs running, but reduce the resource requirements
when the graph is not active (especially memory).

This could be extended to support cloud-based computing where components are
only resumed when there is work to do (spawning a new pod on demand). Then 
suspending once the core work is done for the period allowing the pod to be spun 
down.

Finally, this could be tied to the persistence feature to ensure that 
data is stored ready to restore the state.

