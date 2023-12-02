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
    def __init__(self, rabbit_host, log_storer):
        self.rabbit_host = rabbit_host
        self.connection = CommunicationConnection(self.rabbit_host)
        self.log_storer = log_storer

    def initialize_receiver(
        self,
        input,
        input_type,
        replica_id,
        replicas_count,
        routing_key="",
        input_diff_name="",
        delimiter=",",
        client_id="",
        restore_state=None,
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
            client_id=str(client_id),
        )
        if input_type == "QUEUE":
            communication_receiver = CommunicationReceiverQueue(
                communication_receiver_config,
                self.connection,
                self.log_storer,
                messages_received_restore_state=restore_state.get_messages_received()
                if restore_state
                else {},
                possible_duplicates_restore_state=restore_state.get_possible_duplicates()
                if restore_state
                else {},
            )
        elif input_type == "EXCHANGE":
            communication_receiver = CommunicationReceiverExchange(
                communication_receiver_config,
                self.connection,
                self.log_storer,
                messages_received_restore_state=restore_state.get_messages_received()
                if restore_state
                else {},
                possible_duplicates_restore_state=restore_state.get_possible_duplicates()
                if restore_state
                else {},
            )
        return communication_receiver

    def initialize_sender(
        self,
        output,
        output_type,
        delimiter=",",
        restore_state=None,
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
                self.log_storer,
                messages_sent_restore_state=restore_state.get_messages_sent()
                if restore_state
                else {},
            )
        elif output_type == "EXCHANGE":
            communication_sender = CommunicationSenderExchange(
                communication_sender_config,
                self.connection,
                self.log_storer,
                messages_sent_restore_state=restore_state.get_messages_sent()
                if restore_state
                else {},
            )
        return communication_sender
