from joiner import Joiner
from commons.communication import Communication, CommunicationConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "lat_long_input_queue": str,
        "vuelos_input_queue": str,
        "output_queue": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    communication_lat_long_config = CommunicationConfig(
        config_params["lat_long_input_queue"],
        None,
        config_params["rabbit_host"],
        config_params["input_type"],
        None,
        config_params["replicas_count"],
    )

    communication_distancia_config = CommunicationConfig(
        config_params["vuelos_input_queue"],
        config_params["output_queue"],
        config_params["rabbit_host"],
        config_params["input_type"],
        config_params["output_type"],
        config_params["replicas_count"],
    )
    communication_lat_long = Communication(communication_lat_long_config)
    communication_distancia = Communication(communication_distancia_config)

    joiner = Joiner(communication_lat_long, communication_distancia)
    joiner.run()


if __name__ == "__main__":
    main()
