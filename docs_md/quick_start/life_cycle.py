from hgraph import compute_node, TS
from hgraph.test import eval_node


@compute_node
def life_cycle_example(a: TS[str]) -> TS[str]:
    print("Evaluating")
    return a.value


@life_cycle_example.start
def life_cycle_example_start():
    print("Start")


@life_cycle_example.stop
def life_cycle_example_stop():
    print("Stop")


print(
    eval_node(
        life_cycle_example,
        a=[
            "Hello",
        ],
    )
)
