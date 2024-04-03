from hgraph import DataWriter, DataReader


class InMemoryDataWriter(DataWriter):

    def __init__(self):
        self._data: bytes = bytes()

    def write(self, b: bytes):
        self._data += b

    def as_bytes(self) -> bytes:
        return self._data


class InMemoryDataReader(DataReader):

    def __init__(self, data: bytes):
        self._data: bytes = data
        self._index: int = 0

    def read(self, size: int) -> bytes:
        offset: int = self._index
        self._index += size
        return self._data[offset:self._index]


