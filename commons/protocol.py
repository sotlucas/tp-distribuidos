import logging
import multiprocessing
import socket
from enum import Enum
from commons.message_utils import MessageBytesReader, MessageBytesWriter

BUFFER_SIZE = 8192  # 8 KiB
END_OF_MESSAGE = b"\r\n\r\n"


class MessageType(Enum):
    ANNOUNCE = 0
    PROTOCOL = 1
    RESULT = 2
    EOF = 3


class MessageProtocolType(Enum):
    FLIGHT = 0
    AIRPORT = 1


class Message:
    def __init__(self, message_type):
        self.message_type = message_type

    def from_bytes(bytes):
        """
        Parse the message and return a Message object
        """
        reader = MessageBytesReader(bytes)

        type = reader.read_int(1)

        if type == MessageType.ANNOUNCE.value:
            return AnnounceMessage.from_bytes(reader)
        elif type == MessageType.PROTOCOL.value:
            return ClientProtocolMessage.from_bytes(reader)
        elif type == MessageType.RESULT.value:
            return ResultMessage.from_bytes(reader)
        elif type == MessageType.EOF.value:
            return EOFMessage.from_bytes(reader)
        else:
            raise Exception("Unknown message type")

    def to_bytes(self):
        writer = MessageBytesWriter()

        writer.write_int(self.message_type.value, 1)

        return self.to_bytes_impl(writer) + END_OF_MESSAGE

    def to_bytes_impl(self, writer):
        raise NotImplementedError(
            "to_bytes_impl not implemented, subclass must implement it"
        )


class AnnounceMessage(Message):
    pass


class ClientProtocolMessage(Message):
    def __init__(self, message_id, protocol_type: MessageProtocolType, content):
        super().__init__(MessageType.PROTOCOL)
        self.message_id = message_id
        self.protocol_type = protocol_type
        self.content = content

    def from_bytes(reader):
        message_id = reader.read_int(8)
        protocol_type_value = reader.read_int(1)
        protocol_type = MessageProtocolType(protocol_type_value)
        content_bytes = reader.read_to_end()
        content = content_bytes.decode("utf-8")

        return ClientProtocolMessage(message_id, protocol_type, content)

    def to_bytes_impl(self, writer):
        writer.write_int(self.message_id, 8)
        writer.write_int(self.protocol_type.value, 1)
        writer.write(self.content.encode("utf-8"))
        return writer.get_bytes()


class ResultMessage(Message):
    def __init__(self, result):
        super().__init__(MessageType.RESULT)
        self.result = result

    def from_bytes(reader):
        result_bytes = reader.read_to_end()
        result = result_bytes.decode("utf-8")

        return ResultMessage(result)

    def to_bytes_impl(self, writer):
        writer.write(self.result.encode("utf-8"))
        return writer.get_bytes()


class EOFMessage(Message):
    def __init__(self, protocol_type: MessageProtocolType):
        super().__init__(MessageType.EOF)
        self.protocol_type = protocol_type

    def from_bytes(reader):
        protocol_type_value = reader.read_int(1)
        protocol_type = MessageProtocolType(protocol_type_value)
        return EOFMessage(protocol_type)

    def to_bytes_impl(self, writer):
        writer.write_int(self.protocol_type.value, 1)
        return writer.get_bytes()


class CommunicationBuffer:
    def __init__(self, sock):
        self.sock = sock
        self.sock.setblocking(True)
        self.buffer = b""
        self.lock = multiprocessing.Lock()

    def get_message(self):
        """
        Get a message from the socket.
        """
        while END_OF_MESSAGE not in self.buffer:
            data = self.sock.recv(BUFFER_SIZE)
            if not data:  # socket is closed
                raise PeerDisconnected
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(END_OF_MESSAGE)
        return Message.from_bytes(line)

    def send_message(self, message: Message):
        """
        Send a message through the socket.
        """
        with self.lock:
            self.sock.sendall(message.to_bytes())

    def send_eof(self, eof_type: MessageProtocolType):
        """
        Send an EOF message through the socket.
        """
        message = EOFMessage(eof_type)
        self.send_message(message)

    def stop(self):
        """
        Graceful shutdown. Closing all connections.
        """
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()


class PeerDisconnected(Exception):
    pass
