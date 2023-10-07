BUFFER_SIZE = 8192  # 8 KiB
END_OF_MESSAGE = b"\r\n\r\n"


class CommunicationBuffer:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def get_line(self):
        while END_OF_MESSAGE not in self.buffer:
            data = self.sock.recv(BUFFER_SIZE)
            if not data or data[-1] == 0:  # socket is closed
                raise PeerDisconnected
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(END_OF_MESSAGE)
        return line.decode()


class PeerDisconnected(Exception):
    pass
