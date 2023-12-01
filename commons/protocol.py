from enum import Enum
from commons.message_utils import MessageBytesReader, MessageBytesWriter


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

        return self.to_bytes_impl(writer)

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
