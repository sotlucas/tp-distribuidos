import logging

BUFFER_SIZE = 8192  # 8 KiB
END_OF_MESSAGE = b"\r\n\r\n"


class Message:
    def __init__(self, type, content):
        self.type = type
        self.content = content


class CommunicationBuffer:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def get_line(self):
        while END_OF_MESSAGE not in self.buffer:
            data = self.sock.recv(BUFFER_SIZE)
            if not data:  # socket is closed
                raise PeerDisconnected
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(END_OF_MESSAGE)
        if line == b"\2\0":
            raise PeerDisconnected
        return self.__parse_message(line)

    def __parse_message(self, line):
        if line[0] == 1:
            message_type = "airport"
            message = line[1:]
        elif line[0] == 2:
            message_type = "flight"
            message = line[1:]
        else:
            message_type = None
            message = line
        return Message(message_type, message)


class PeerDisconnected(Exception):
    pass
