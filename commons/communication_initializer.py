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
        replica_id,
        replicas_count,
        routing_key="",
        input_diff_name="",
        delimiter=",",
        messages_received_restore_state={},
        possible_duplicates_restore_state={},
        log_storer_suffix="",
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
        )
        if input_type == "QUEUE":
            communication_receiver = CommunicationReceiverQueue(
                communication_receiver_config,
                self.connection,
                messages_received_restore_state=messages_received_restore_state,
                possible_duplicates_restore_state=possible_duplicates_restore_state,
                log_storer_suffix=log_storer_suffix,
            )
        elif input_type == "EXCHANGE":
            communication_receiver = CommunicationReceiverExchange(
                communication_receiver_config,
                self.connection,
                messages_received_restore_state=messages_received_restore_state,
                possible_duplicates_restore_state=possible_duplicates_restore_state,
                log_storer_suffix=log_storer_suffix,
            )
        return communication_receiver

    def initialize_sender(
        self,
        output,
        output_type,
        delimiter=",",
        messages_sent_restore_state={},
        log_storer_suffix="",
    ):
        """
        Initialize the sender based on the output type
        """
        communication_sender_config = CommunicationSenderConfig(
            output, delimiter=delimiter
        )
        if output_type == "QUEUE":
            communication_sender = CommunicationSenderQueue(
                communication_sender_config,
                self.connection,
                messages_sent_restore_state=messages_sent_restore_state,
                log_storer_suffix=log_storer_suffix,
            )
        elif output_type == "EXCHANGE":
            communication_sender = CommunicationSenderExchange(
                communication_sender_config,
                self.connection,
                messages_sent_restore_state=messages_sent_restore_state,
                log_storer_suffix=log_storer_suffix,
            )
        return communication_sender
