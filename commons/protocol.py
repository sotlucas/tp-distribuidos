from enum import Enum
from commons.message_utils import MessageBytesReader, MessageBytesWriter

"""
Messages used by the communication protocol between the client and the server.
"""


class MessageType(Enum):
    ANNOUNCE = 0
    PROTOCOL = 1
    RESULT = 2
    EOF = 3
    HEALTH_CHECK = 4
    HEALTH_OK = 5
    ACK = 6
    ANNOUNCE_ACK = 7


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
        elif type == MessageType.HEALTH_CHECK.value:
            return HealthCheckMessage.from_bytes(reader)
        elif type == MessageType.HEALTH_OK.value:
            return HealthOkMessage.from_bytes(reader)
        elif type == MessageType.ACK.value:
            return ACKMessage.from_bytes(reader)
        elif type == MessageType.ANNOUNCE_ACK.value:
            return AnnounceACKMessage.from_bytes(reader)
        else:
            raise Exception("Unknown message type")

    def to_bytes(self):
        writer = MessageBytesWriter()

        writer.write_int(self.message_type.value, 1)

        return self.to_bytes_impl(writer)

    def to_bytes_impl(self, writer):
        raise NotImplementedError(
            "to_bytes_impl not implemented, subclass must implement it"
        )


class AnnounceMessage(Message):
    def __init__(self, client_id):
        super().__init__(MessageType.ANNOUNCE)
        self.client_id = client_id

    def from_bytes(reader):
        client_id = reader.read_int(8)
        return AnnounceMessage(client_id)

    def to_bytes_impl(self, writer):
        writer.write_int(self.client_id, 8)
        return writer.get_bytes()


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

    def __str__(self):
        return f"ClientProtocolMessage(message_id={self.message_id}, protocol_type={self.protocol_type})"


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

    def __str__(self):
        return f"EOFMessage(protocol_type={self.protocol_type})"


class HealthCheckMessage(Message):
    def __init__(self):
        super().__init__(MessageType.HEALTH_CHECK)

    def from_bytes(reader):
        return HealthCheckMessage()

    def to_bytes_impl(self, writer):
        return writer.get_bytes()


class HealthOkMessage(Message):
    def __init__(self):
        super().__init__(MessageType.HEALTH_OK)

    def from_bytes(reader):
        return HealthOkMessage()

    def to_bytes_impl(self, writer):
        return writer.get_bytes()


class AnnounceACKMessage(Message):
    def __init__(self):
        super().__init__(MessageType.ANNOUNCE_ACK)

    def from_bytes(reader):
        return AnnounceACKMessage()

    def to_bytes_impl(self, writer):
        return writer.get_bytes()


class ACKMessage(Message):
    def __init__(self, message_id, protocol_type: MessageProtocolType):
        super().__init__(MessageType.ACK)
        self.message_id = message_id
        self.protocol_type = protocol_type

    def from_bytes(reader):
        message_id = reader.read_int(8)
        protocol_type_value = reader.read_int(1)
        protocol_type = MessageProtocolType(protocol_type_value)
        return ACKMessage(message_id, protocol_type)

    def to_bytes_impl(self, writer):
        writer.write_int(self.message_id, 8)
        writer.write_int(self.protocol_type.value, 1)
        return writer.get_bytes()

    def __str__(self):
        return f"ACKMessage(message_id={self.message_id}, protocol_type={self.protocol_type})"
