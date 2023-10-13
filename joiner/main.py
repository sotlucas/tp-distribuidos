from joiner import Joiner
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import initialize_receiver, initialize_sender


def main():
    config_inputs = {
        "lat_long_input": str,
        "vuelos_input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    lat_long_receiver = initialize_receiver(
        config_params["rabbit_host"],
        config_params["lat_long_input"],
        config_params["replicas_count"],
        config_params["input_type"],
    )

    vuelos_receiver = initialize_receiver(
        config_params["rabbit_host"],
        config_params["vuelos_input"],
        config_params["replicas_count"],
        config_params["input_type"],
    )

    vuelos_sender = initialize_sender(
        config_params["rabbit_host"],
        config_params["output"],
        config_params["output_type"],
    )

    joiner = Joiner(lat_long_receiver, vuelos_receiver, vuelos_sender)
    joiner.run()


if __name__ == "__main__":
    main()
