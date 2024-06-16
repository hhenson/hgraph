class WiringObserver:
    """
    Observer for wiring events. When installed in the wiring engine, the observer will receive notifications from wiring process
    """

    def on_enter_graph_wiring(self, signature: "WiringNodeSignature"):
        pass

    def on_exit_graph_wiring(self, signature: "WiringNodeSignature", error):
        pass

    def on_enter_nested_graph_wiring(self, signature: "WiringNodeSignature"):
        pass

    def on_exit_nested_graph_wiring(self, signature: "WiringNodeSignature", error):
        pass

    def on_enter_node_wiring(self, signature: "WiringNodeSignature"):
        pass

    def on_exit_node_wiring(self, signature: "WiringNodeSignature", error):
        pass

    def on_overload_resolution(
        self, signature: "WiringNodeSignature", selected_overload, rejected_overloads, ambiguous_overloads
    ):
        pass


class WiringObserverContext:
    """
    Context manager for wiring observer.
    """

    _instance = None

    def __init__(self):
        self._observers = []

    def __enter__(self):
        self.__class__._instance = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__class__._instance = None

    @classmethod
    def instance(cls) -> "WiringObserverContext":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def add_wiring_observer(self, observer: WiringObserver):
        self._observers.append(observer)

    def remove_wiring_observer(self, observer: WiringObserver):
        self._observers.remove(observer)

    def notify_enter_graph_wiring(self, signature: "WiringNodeSignature"):
        for observer in self._observers:
            observer.on_enter_graph_wiring(signature)

    def notify_exit_graph_wiring(self, signature: "WiringNodeSignature", error):
        for observer in self._observers:
            observer.on_exit_graph_wiring(signature, error)

    def notify_enter_nested_graph_wiring(self, signature: "WiringNodeSignature"):
        for observer in self._observers:
            observer.on_enter_nested_graph_wiring(signature)

    def notify_exit_nested_graph_wiring(self, signature: "WiringNodeSignature", error):
        for observer in self._observers:
            observer.on_exit_nested_graph_wiring(signature, error)

    def notify_enter_node_wiring(self, signature: "WiringNodeSignature"):
        for observer in self._observers:
            observer.on_enter_node_wiring(signature)

    def notify_exit_node_wiring(self, signature: "WiringNodeSignature", error):
        for observer in self._observers:
            observer.on_exit_node_wiring(signature, error)

    def notify_overload_resolution(
        self, signature: "WiringNodeSignature", selected_overload, rejected_overloads, ambiguous_overloads
    ):
        for observer in self._observers:
            observer.on_overload_resolution(signature, selected_overload, rejected_overloads, ambiguous_overloads)
