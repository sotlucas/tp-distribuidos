from grouper import Grouper
from commons.communication import Communication, CommunicationConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "vuelos_input_queue": str,
        "vuelos_output_queue": str,
        "media_general_input_queue": str,
        "media_general_output_queue": str,
        "logging_level": str,
        "rabbit_host": str,
        "input_type": str,
        "output_type": str,
        "replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    communication_vuelos_config = CommunicationConfig(
        config_params["vuelos_input_queue"],
        config_params["vuelos_output_queue"],
        config_params["rabbit_host"],
        config_params["input_type"],
        config_params["output_type"],
        config_params["replicas_count"],
    )
    communication_vuelos = Communication(communication_vuelos_config)

    communication_media_general_config = CommunicationConfig(
        config_params["media_general_input_queue"],
        config_params["media_general_output_queue"],
        config_params["rabbit_host"],
        config_params["input_type"],
        config_params["output_type"],
        config_params["replicas_count"],
    )
    communication_media_general = Communication(communication_media_general_config)

    processor = Grouper(communication_vuelos, communication_media_general)
    processor.run()


if __name__ == "__main__":
    main()
