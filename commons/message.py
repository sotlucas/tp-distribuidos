from enum import Enum


class MessageType(Enum):
    FLIGHT = 0
    EOF = 1
    EOF_REQUEUE = 2
    EOF_CALLBACK = 3


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

        if type == MessageType.FLIGHT:
            return FlightMessage.from_bytes(client_id, bytes)
        elif type == MessageType.EOF:
            return EOFMessage.from_bytes(client_id, bytes)
        elif type == MessageType.EOF_REQUEUE:
            return EOFRequeueMessage.from_bytes(client_id, bytes)
        elif type == MessageType.EOF_CALLBACK:
            return EOFCallbackMessage.from_bytes(client_id, bytes)
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


class FlightMessage(Message):
    """
    Flight message structure:

        0      2          10         N
        | type | client_id | payload |

    """

    def __init__(self, client_id, payload_bytes):
        message_type = MessageType.FLIGHT
        super().__init__(message_type, client_id)
        self.payload_bytes = payload_bytes

    def from_bytes(self, client_id, bytes):
        flight_payload = bytes[10:]

        return FlightMessage(client_id, flight_payload)

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        return message_type_bytes + client_id_bytes + self.payload_bytes


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

        0      2          10     14                  22              30                     38
        | type | client_id | TTL | remaining_messages | messages_sent | messages_sent_sender |

    """

    def __init__(
        self, client_id, ttl, remaining_messages, messages_sent, messages_sent_sender
    ):
        message_type = MessageType.EOF_REQUEUE
        super().__init__(message_type, client_id)
        self.ttl = ttl
        self.remaining_messages = remaining_messages
        self.messages_sent = messages_sent
        self.messages_sent_sender = messages_sent_sender

    def from_bytes(client_id, bytes):
        ttl = int.from_bytes(bytes[10:14], byteorder="big")
        remaining_messages = int.from_bytes(bytes[14:22], byteorder="big")
        messages_sent = int.from_bytes(bytes[22:30], byteorder="big")
        messages_sent_sender = int.from_bytes(bytes[30:38], byteorder="big")

        return EOFRequeueMessage(
            client_id, ttl, remaining_messages, messages_sent, messages_sent_sender
        )

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        ttl_bytes = self.ttl.to_bytes(4, byteorder="big")
        remaining_messages_bytes = self.remaining_messages.to_bytes(8, byteorder="big")
        messages_sent_bytes = self.messages_sent.to_bytes(8, byteorder="big")
        messages_sent_sender_bytes = self.messages_sent_sender.to_bytes(
            8, byteorder="big"
        )

        return (
            message_type_bytes
            + client_id_bytes
            + ttl_bytes
            + remaining_messages_bytes
            + messages_sent_bytes
            + messages_sent_sender_bytes
        )


class EOFCallbackMessage(Message):
    """
    EOF callback message structure:

        0      2          10
        | type | client_id |

    """

    def __init__(self, client_id):
        message_type = MessageType.EOF_CALLBACK
        super().__init__(message_type, client_id)

    def from_bytes(client_id, bytes):
        return EOFCallbackMessage(client_id)

    def to_bytes_impl(self, message_type_bytes, client_id_bytes):
        return message_type_bytes + client_id_bytes
