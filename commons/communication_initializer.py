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


def initialize_receiver(
    rabbit_host, input, replicas_count, input_type, routing_key="", output=""
):
    """
    Initialize the receiver based on the input type
    """
    communication_receiver_config = CommunicationReceiverConfig(
        rabbit_host,
        input,
        replicas_count,
    )
    if input_type == "QUEUE":
        communication_receiver = CommunicationReceiverQueue(
            communication_receiver_config
        )
    elif input_type == "EXCHANGE":
        communication_receiver = CommunicationReceiverExchange(
            communication_receiver_config, routing_key, output
        )
    return communication_receiver


def initialize_sender(rabbit_host, output, output_type):
    """
    Initialize the sender based on the output type
    """
    communication_sender_config = CommunicationSenderConfig(rabbit_host, output)
    if output_type == "QUEUE":
        communication_sender = CommunicationSenderQueue(communication_sender_config)
    elif output_type == "EXCHANGE":
        communication_sender = CommunicationSenderExchange(communication_sender_config)
    return communication_sender
