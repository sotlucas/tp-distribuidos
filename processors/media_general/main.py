from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from media_general import MediaGeneral
from commons.connection import ConnectionConfig, Connection

MEDIA_GENERAL_REPLICAS_COUNT = 1


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "grouper_replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    communication_initializer = CommunicationInitializer(config_params["rabbit_host"])
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        MEDIA_GENERAL_REPLICAS_COUNT,
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    input_fields = ["sum", "amount"]
    output_fields = ["media_general"]

    processor = MediaGeneral(config_params["grouper_replicas_count"])

    connection_config = ConnectionConfig(input_fields, output_fields)
    Connection(connection_config, receiver, sender, processor).run()


if __name__ == "__main__":
    main()
