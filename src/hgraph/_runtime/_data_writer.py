from abc import abstractmethod, ABC
from datetime import date, datetime, timedelta, time

__all__ = ("DataWriter", "DataReader", "Persistable")


class DataWriter(ABC):

    @abstractmethod
    def write(self, b: bytes):
        ...

    def write_bytes(self, b: bytes):
        """Writes the bytes including the count to enable reloading"""
        self.write_int(len(b))
        self.write(b)

    def write_date(self, d: date):
        self.write((d.year * 10000 + d.month * 100 + d.day).to_bytes(32 // 8, byteorder='big', signed=True))

    def write_datetime(self, d: datetime):
        self.write_float(d.timestamp())

    def write_time_delta(self, td: timedelta):
        self.write(
            (int(td.total_seconds()) * 1000000 + td.microseconds).to_bytes(64 // 8, byteorder='big', signed=True))

    def write_float(self, f: float):
        import struct
        self.write(struct.pack('d', f))

    def write_int(self, i: int):
        self.write(i.to_bytes(64 // 8, byteorder='big', signed=True))

    def write_string(self, s: str):
        b = s.encode('utf-8')
        self.write_int(len(b))
        self.write(b)

    def write_boolean(self, b: bool):
        self.write(b'T' if b else b'F')

    def write_time(self, tm: time):
        # only need 37 bits, but since it is more than 32 will pack into a 64 bit structure
        self.write((((tm.hour * 3600 + tm.minute * 60 + tm.second) * 1000000) +
                    tm.microsecond).to_bytes(64 // 8, byteorder='big', signed=True))


class DataReader(ABC):

    @abstractmethod
    def read(self, size: int) -> bytes:
        ...

    def read_bytes(self) -> bytes:
        sz = self.read_int()
        return self.read(sz)

    def read_date(self) -> date:
        i = int.from_bytes(self.read(32 // 8), "big", signed=True)
        year = i // 10000
        month = i // 100 - year * 10000
        day = i - year * 10000 - month * 100
        return date(year, month, day)

    def read_datetime(self) -> datetime:
        ts = self.read_float()
        return datetime.fromtimestamp(ts)

    def read_time_delta(self) -> timedelta:
        i = self.read_int()
        seconds = i // 1000000
        microseconds = i - seconds
        return timedelta(seconds=seconds, microseconds=microseconds)

    def read_float(self) -> float:
        import struct
        return struct.unpack('d', self.read(64 // 8))[0]

    def read_int(self) -> int:
        return int.from_bytes(self.read(64 // 8), "big", signed=True)

    def read_string(self) -> str:
        sz = self.read_int()
        b = self.read(sz)
        return str(b, encoding='utf-8')

    def read_boolean(self) -> bool:
        b = self.read(1)
        return bool('T' == b)

    def read_time(self) -> time:
        i = self.read_int()
        hr = i // 3600
        min = i // 60 - hr * 3600
        sec = i - hr * 3600 - min * 60
        return time(hr, min, sec)


class Persistable(ABC):

    @abstractmethod
    def write_value(self, writer: DataWriter):
        """
        Write the full value into the data writer.
        """

    @abstractmethod
    def read_value(self, tm: datetime, reader: DataReader):
        """
        Read the value as a full value.
        The `tm` parameter is the time to use for the restoration of the value.
        """
