from grouper import Grouper
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer


def main():
    config_inputs = {
        "vuelos_input": str,
        "vuelos_output": str,
        "media_general_input": str,
        "media_general_output": str,
        "logging_level": str,
        "rabbit_host": str,
        "input_type": str,
        "output_type": str,
        "replicas_count": int,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    vuelos_communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"]
    )
    vuelos_receiver = vuelos_communication_initializer.initialize_receiver(
        config_params["vuelos_input"],
        config_params["input_type"],
        config_params["replicas_count"],
        routing_key=str(config_params["replica_id"]),
    )
    vuelos_sender = vuelos_communication_initializer.initialize_sender(
        config_params["vuelos_output"], config_params["output_type"]
    )

    media_general_communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"]
    )
    media_general_receiver = (
        media_general_communication_initializer.initialize_receiver(
            config_params["media_general_input"],
            config_params["input_type"],
            config_params["replicas_count"],
        )
    )
    media_general_sender = media_general_communication_initializer.initialize_sender(
        config_params["media_general_output"], config_params["output_type"]
    )

    processor = Grouper(
        config_params["replica_id"],
        vuelos_receiver,
        vuelos_sender,
        media_general_receiver,
        media_general_sender,
    )
    processor.run()


if __name__ == "__main__":
    main()
