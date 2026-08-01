import threading

from hgraph import REMOVE

__all__ = ("KeyedAsyncState",)


class KeyedAsyncState:
    """Generation-safe bridge from worker completions to a keyed push source."""

    def __init__(self):
        self.lock = threading.Lock()
        self.sender = None
        self.pending = []
        self.generations = {}
        self.published = set()
        self.active = True

    def attach(self, sender):
        with self.lock:
            if not self.active:
                return
            self.sender = sender
            pending, self.pending = self.pending, []
        for value in pending:
            sender(value)

    def begin(self, key):
        with self.lock:
            generation = self.generations.get(key, 0) + 1
            self.generations[key] = generation
            return generation

    def publish(self, key, generation, value):
        with self.lock:
            if not self.active or self.generations.get(key) != generation:
                return False
            self.published.add(key)
            delta = {key: value}
            if self.sender is None:
                self.pending.append(delta)
                return True
            sender = self.sender
        sender(delta)
        return True

    def cancel(self, key):
        with self.lock:
            self.generations[key] = self.generations.get(key, 0) + 1
            if key not in self.published:
                return False
            self.published.remove(key)
            delta = {key: REMOVE}
            if self.sender is None:
                self.pending.append(delta)
                return True
            sender = self.sender
        sender(delta)
        return True

    def close(self):
        with self.lock:
            self.active = False
            self.sender = None
            self.pending.clear()
            self.generations.clear()
            self.published.clear()
