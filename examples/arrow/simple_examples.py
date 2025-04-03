from hgraph import graph, const, debug_print, convert, TSL, TS, Size, collect, add_
from hgraph.arrow import arrow, i
from hgraph.arrow._pair_operators import first, swap, second
from hgraph.arrow._std_operators import binary_op
from hgraph.arrow._test_operators import assert_
from hgraph.test import eval_node


@arrow
def print_out(x):
    debug_print("out", x)


@graph
def simple_print():
    arrow(1) | print_out


@graph
def sum_and_print_1():
    arrow(1, 2) | i >> (lambda pair: pair[0] + pair[1]) >> print_out


@graph
def sum_and_print_2():
    sum_and_print = i >> (lambda pair: pair[0] + pair[1]) >> print_out
    sum_and_print(TSL.from_ts(const(1), const(2)))


@graph
def fan_out():
    arrow(1) | i / i >> print_out


@graph
def cross():
    arrow(1, 2) | i >> arrow(lambda x: x + 1) // (lambda x: x + 2) >> print_out


@graph
def first_and_second():
    arrow(1, 2) | first >> print_out
    arrow(1, 2) | second >> print_out


@graph
def swap_():
    arrow(1, 2) | swap >> print_out


@graph
def binary_op_():
    arrow(1, 2) | binary_op(add_) >> print_out


@graph
def assert_logic():
    arrow(1, 2) | binary_op(add_) >> assert_(3) >> print_out


if __name__ == "__main__":
    eval_node(simple_print)
    eval_node(sum_and_print_1)
    eval_node(sum_and_print_2)
    eval_node(fan_out)
    eval_node(cross)
    eval_node(first_and_second)
    eval_node(binary_op_)
    eval_node(assert_logic)
