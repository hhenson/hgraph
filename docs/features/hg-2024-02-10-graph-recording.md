hg-2024-02-10-recording
=======================

Recording of the graph time-series values can be useful for a number of reasons,
namely:
* Replay to support debugging, forensics and performance tuning.
* Supporting weak regression testing.
* Support suspend-resume functionality

The naive approach to implementation would be to record every time-series in
the graph, but this would waste resources and be slow. Additionally, this would
not allow for a light-weight 'resume' as it would not deal with state properties.

A solution would be to target selected elements of the graph to record. What is recorded
will depend on the objective.

The following scenarios are considered:

### Replay

In this scenario, we are looking to replay the values into the graph and allow 
the graph to re-create its state. This is the most light-weight solution. 
It only requires source nodes to be recorded. But replaying requires the 
graph to be replayed from the first tick until the point-in-time required.
Useful for debugging.

### Restore

In this scenario, we record all elements of state in the system (i.e all 
source-nodes as well as state, outputs that are consumed by inputs and inputs
such as clock-responses). This allows the graph to be recoved by playing back the
last recorded results and then ticking the next set of values into the graph.

With state being recorded, it is possible to ensure that the graph will evaluate
the last result exactly as it was re-establishing the state of the graph to 
the point it was when it was suspended.

### Validate

The most simple form of validation is to replay the state by replaying the values
of the source nodes and ensuring that the values provided to the sink-nodes
match those previously performed.


## The 'Steins Gate' world lines

The anime 'Steins Gate' provides an interesting way to think about graph-replay.
We can think of the first run of a graph to be the original / first world line
created. If we never ran the graph again, then this would be the only time-line
to exist; however, there are scenarios where we wish to make small alterations and
rerun the graph for all or some of the time. This creates an alternative 
world line (or in our case, a new recording of the graph state within an existing
time period).

We need to be able to identify time-lines and identify the divergences that occur.
In a production environment, we often consider there to be a single time-line. 
This is artificial as in reality we are constantly shifting things. Examples of change
include: code, configuration, source-data.

Let's consider the case of computing PnL, we use inputs such as market data to
compute the values, there are times when the market data we used is incorrect and
gets re-stated. We now have two world lines created, one where we computed the price
with the value received at the point-in-time and one where we compute the result
with the actual value that existed at the point-in-time. The results of the
two computations are different, but both are meaningful. We need to know about the
value we computed originally, but we also would like to have the corrected view
as it more closely identifies the *truth*. However, we may choose to create a third 
view. This view uses the time-line up to the point where we identified the issue,
and then the switch to the time-line that should have existed if we had received
the correct values. This we could refer to as an aliased time-line, which we may
refer to as the PROD time-line. 

The original time-line may now no longer be computed further and so comes to an end.

Other examples we could consider are those of code change. This is more complicated,
in that, we can either start a new world line at the point where the code is released;
or we could re-boot the world line from inception and re-evaluate the results
to create a completely new world line, in this case, what do we do if the code
produces the same result for part of the ramp up?

Configuration changes are also similarly interesting.

The next challenge is that we may wish to run *'what-if'* scenarios, these create
world lines used to analyse the differences that occur as a result of changing one or 
more aspects of world (or computation). All these time-line recordings need to be
uniquely identified and tracked. Allowing us to review and compare results without
loosing sight of one or more key world-line views.





   