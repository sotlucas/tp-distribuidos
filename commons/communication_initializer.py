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
    def __init__(self, rabbit_host, log_guardian):
        self.rabbit_host = rabbit_host
        self.connection = CommunicationConnection(self.rabbit_host)
        self.log_guardian = log_guardian

    def initialize_receiver(
        self,
        input,
        input_type,
        replica_id,
        replicas_count,
        routing_key="",
        input_diff_name="",
        delimiter=",",
        use_duplicate_catcher=False,
        load_balancer_send_multiply=None,
    ):
        """
        Initialize the receiver based on the input type
        """
        communication_receiver_config = CommunicationReceiverConfig(
            input,
            replica_id,
            replicas_count,
            routing_key=routing_key,
            input_diff_name=input_diff_name,
            delimiter=delimiter,
            use_duplicate_catcher=use_duplicate_catcher,
            load_balancer_send_multiply=load_balancer_send_multiply,
        )
        if input_type == "QUEUE":
            communication_receiver = CommunicationReceiverQueue(
                communication_receiver_config, self.connection, self.log_guardian
            )
        elif input_type == "EXCHANGE":
            communication_receiver = CommunicationReceiverExchange(
                communication_receiver_config, self.connection, self.log_guardian
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
                communication_sender_config, self.connection, self.log_guardian
            )
        elif output_type == "EXCHANGE":
            communication_sender = CommunicationSenderExchange(
                communication_sender_config, self.connection, self.log_guardian
            )
        return communication_sender
