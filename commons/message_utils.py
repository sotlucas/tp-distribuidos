class MessageBytesReader:
    def __init__(self, bytes):
        self.buffer = bytes
        self.offset = 0

    def read(self, size):
        if self.offset + size > len(self.buffer):
            raise Exception("Not enough bytes to read")

        bytes = self.buffer[self.offset : self.offset + size]
        self.offset += size
        return bytes

    def read_int(self, size):
        bytes = self.read(size)
        return int.from_bytes(bytes, byteorder="big")

    def read_multiple_int(self, size, count):
        ints = []
        for i in range(count):
            ints.append(self.read_int(size))
        return ints

    def read_to_end(self):
        bytes = self.buffer[self.offset :]
        self.offset = len(self.buffer)
        return bytes


class MessageBytesWriter:
    def __init__(self):
        self.buffer = b""

    def write(self, bytes):
        self.buffer += bytes

    def write_int(self, value, size):
        bytes = value.to_bytes(size, byteorder="big")
        self.write(bytes)

    def write_multiple_int(self, values, size):
        for value in values:
            self.write_int(value, size)

    def get_bytes(self):
        return self.buffer
