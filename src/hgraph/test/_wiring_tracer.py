from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_observer import WiringObserver

__all__ = ("WiringTracer",)


class WiringTracer(WiringObserver):
    def __init__(self, filter: str = None, graph: bool = True, node: bool = True):
        self.filter = filter
        self.graph = graph
        self.node = node

    def on_enter_graph_wiring(self, signature: "WiringNodeSignature"):
        if self.graph and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            print(f"Wiring graph {signature.signature}")

    def on_exit_graph_wiring(self, signature: "WiringNodeSignature", error):
        if self.graph and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            print(f"Done wiring graph {signature.signature}")

    def on_enter_nested_graph_wiring(self, signature: "WiringNodeSignature"):
        if self.graph and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            print(f"Wiring nested graph {signature.signature}")

    def on_exit_nested_graph_wiring(self, signature: "WiringNodeSignature", error):
        if self.graph and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            print(f"Done wiring nested graph {signature.signature}")

    def on_enter_node_wiring(self, signature: "WiringNodeSignature"):
        if self.node and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            print(f"Wiring node {signature.signature}")

    def on_exit_node_wiring(self, signature: "WiringNodeSignature", error):
        if self.node and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            if error:
                print(f"Error wiring node {signature.signature}: {error}")
            else:
                print(f"Done wiring node {signature.signature}")

    def on_overload_resolution(
        self, signature: "WiringNodeSignature", selected_overload, rejected_overloads, ambiguous_overloads
    ):
        if self.node and (self.filter is None or self.filter in WiringGraphContext.wiring_path_name()):
            print(f"Overload resolution for {signature.name} " + "successful" if selected_overload else "failed")
            if selected_overload:
                print(f"Selected overload: {selected_overload[0]}, rank {selected_overload[1]}")
            if ambiguous_overloads:
                print(f"Candidate overloads: \n" + "\n\t".join(f"{o}: {r}" for o, r in ambiguous_overloads))
            if rejected_overloads:
                print(f"Rejected overloads: \n\t" + "\n\t".join(f"{o}: {e}" for o, e in rejected_overloads))
