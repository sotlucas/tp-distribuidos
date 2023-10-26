from commons.communication import CommunicationConnection

from commons.communication import (
    CommunicationReceiverExchange,
    CommunicationReceiverQueue,
    CommunicationReceiverConfig,
)
from commons.communication import (
    CommunicationSenderExchange,
    CommunicationSenderQueue,
    CommunicationSenderConfig,
)


class CommunicationInitializer:
    def __init__(self, rabbit_host):
        self.rabbit_host = rabbit_host
        self.connection = CommunicationConnection(self.rabbit_host)

    def initialize_receiver(
        self,
        input,
        input_type,
        replicas_count,
        routing_key="",
        input_diff_name="",
        replica_id=1,
        delimiter=",",
    ):
        """
        Initialize the receiver based on the input type
        """
        communication_receiver_config = CommunicationReceiverConfig(
            input,
            replicas_count,
            routing_key=routing_key,
            input_diff_name=input_diff_name,
            replica_id=replica_id,
            delimiter=delimiter,
        )
        if input_type == "QUEUE":
            communication_receiver = CommunicationReceiverQueue(
                communication_receiver_config, self.connection
            )
        elif input_type == "EXCHANGE":
            communication_receiver = CommunicationReceiverExchange(
                communication_receiver_config, self.connection
            )
        return communication_receiver

    def initialize_sender(self, output, output_type, delimiter=","):
        """
        Initialize the sender based on the output type
        """
        communication_sender_config = CommunicationSenderConfig(
            output, delimiter=delimiter
        )
        if output_type == "QUEUE":
            communication_sender = CommunicationSenderQueue(
                communication_sender_config, self.connection
            )
        elif output_type == "EXCHANGE":
            communication_sender = CommunicationSenderExchange(
                communication_sender_config, self.connection
            )
        return communication_sender
