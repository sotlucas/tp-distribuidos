from enum import Enum


class MessageType(Enum):
    PROTOCOL = 0
    EOF = 1
    EOF_REQUEUE = 2
    EOF_FINISH = 3


class Message:
    def __init__(self, message_type, client_id):
        self.message_type = message_type
        self.client_id = client_id

    def from_bytes(bytes):
        """
        Parse the message and return a Message object
        """
        type = int.from_bytes(bytes[0:2], byteorder="big")
        client_id = int.from_bytes(bytes[2:10], byteorder="big")

        if type == MessageType.PROTOCOL.value:
            return ProtocolMessage.from_bytes(client_id, bytes)
        elif type == MessageType.EOF.value:
            return EOFMessage.from_bytes(client_id, bytes)
        elif type == MessageType.EOF_REQUEUE.value:
            return EOFRequeueMessage.from_bytes(client_id, bytes)
        elif type == MessageType.EOF_FINISH.value:
            return EOFFinishMessage.from_bytes(client_id, bytes)
        else:
            raise Exception("Unknown message type")

    def to_bytes(self):
        message_type_bytes = self.message_type.value.to_bytes(2, byteorder="big")
        client_id_bytes = self.client_id.to_bytes(8, byteorder="big")

        return self.to_bytes_impl(message_type_bytes, client_id_bytes)

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        raise NotImplementedError(
            "to_bytes_impl not implemented, subclass must implement it"
        )


class ProtocolMessage(Message):
    """
    Protocol message structure:

        0      2          10         N
        | type | client_id | payload |

    """

    def __init__(self, client_id, payload):
        message_type = MessageType.PROTOCOL
        super().__init__(message_type, client_id)
        self.payload = payload

    def from_bytes(client_id, bytes):
        payload = bytes[10:]
        payload = payload.decode("utf-8")

        return ProtocolMessage(client_id, payload)

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        payload_bytes = self.payload.encode("utf-8")
        return message_type_bytes + client_id_bytes + payload_bytes


class EOFMessage(Message):
    """
    EOF message structure:

        0      2          10               18
        | type | client_id | messages_sent |

    """

    def __init__(self, client_id, messages_sent):
        message_type = MessageType.EOF
        super().__init__(message_type, client_id)
        self.messages_sent = messages_sent

    def from_bytes(client_id, bytes):
        messages_sent = int.from_bytes(bytes[10:18], byteorder="big")

        return EOFMessage(client_id, messages_sent)

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        messages_sent_bytes = self.messages_sent.to_bytes(8, byteorder="big")

        return message_type_bytes + client_id_bytes + messages_sent_bytes


class EOFRequeueMessage(Message):
    """
    EOF requeue message structure:

        0      2          10     14                  22              30                       38        39
        | type | client_id | TTL | remaining_messages | messages_sent | original_messages_sent | eof_id |

    """

    def __init__(
        self,
        client_id,
        ttl,
        remaining_messages,
        messages_sent,
        original_messages_sent,
        eof_id,
    ):
        message_type = MessageType.EOF_REQUEUE
        super().__init__(message_type, client_id)
        self.ttl = ttl
        self.remaining_messages = remaining_messages
        self.messages_sent = messages_sent
        self.original_messages_sent = original_messages_sent

        # Make sure eof_id is between 0 and 255
        self.eof_id = eof_id % 256

    def from_bytes(client_id, bytes):
        ttl = int.from_bytes(bytes[10:14], byteorder="big")
        remaining_messages = int.from_bytes(bytes[14:22], byteorder="big")
        messages_sent = int.from_bytes(bytes[22:30], byteorder="big")
        original_messages_sent = int.from_bytes(bytes[30:38], byteorder="big")
        eof_id = int.from_bytes(bytes[38:39], byteorder="big")

        return EOFRequeueMessage(
            client_id,
            ttl,
            remaining_messages,
            messages_sent,
            original_messages_sent,
            eof_id,
        )

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        ttl_bytes = self.ttl.to_bytes(4, byteorder="big")
        remaining_messages_bytes = self.remaining_messages.to_bytes(8, byteorder="big")
        messages_sent_bytes = self.messages_sent.to_bytes(8, byteorder="big")
        original_messages_sent_bytes = self.original_messages_sent.to_bytes(
            8, byteorder="big"
        )
        eof_id_bytes = self.eof_id.to_bytes(1, byteorder="big")

        return (
            message_type_bytes
            + client_id_bytes
            + ttl_bytes
            + remaining_messages_bytes
            + messages_sent_bytes
            + original_messages_sent_bytes
            + eof_id_bytes
        )


class EOFFinishMessage(Message):
    """
    EOF finish message structure:

        0      2           10    18
        | type | client_id | ttl |

    """

    def __init__(self, client_id, ttl):
        message_type = MessageType.EOF_FINISH
        super().__init__(message_type, client_id)
        self.ttl = ttl

    def from_bytes(client_id, bytes):
        ttl = int.from_bytes(bytes[10:18], byteorder="big")
        return EOFFinishMessage(client_id, ttl)

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        ttl_bytes = self.ttl.to_bytes(8, byteorder="big")
        return message_type_bytes + client_id_bytes + ttl_bytes
