BUFFER_SIZE = 8192  # 8 KiB
END_OF_MESSAGE = b"\r\n\r\n"


class Message:
    def __init__(self, type, content):
        self.type = type
        self.content = content

    def serialize(self):
        """
        Serialize the message to be sent through the socket.
        """
        return serialize_message(self.type, self.content.encode())


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
        message = parse_message(line)
        if message.type == "flight" and message.content == b"\0":
            raise PeerDisconnected
        return parse_message(line)


def parse_message(line):
    """
    Parse the message received through the socket.
    """
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


def serialize_message(type, content_bytes):
    """
    Serialize the message to be sent through the socket.
    """
    # Add the type of message to the beginning of the line
    if type == "airport":
        type_bytes = int.to_bytes(1, 1, byteorder="big")
    elif type == "flight":
        type_bytes = int.to_bytes(2, 1, byteorder="big")
    return type_bytes + content_bytes + END_OF_MESSAGE


def serialize_eof(type):
    """
    Serialize an EOF message to be sent through the socket.
    """
    return serialize_message(type, b"\0")


class PeerDisconnected(Exception):
    pass
