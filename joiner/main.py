from joiner import Joiner
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer

# Because they are working as an exchange, they do not share the same as the other groupers
# TODO: See if this can be changed
JOINER_REPLICA_COUNT = 1


def main():
    config_inputs = {
        "replica_id": int,
        "lat_long_input": str,
        "vuelos_input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type_lat_long": str,
        "input_type_vuelos": str,
        "replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    lat_long_communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"]
    )
    lat_long_receiver = lat_long_communication_initializer.initialize_receiver(
        config_params["lat_long_input"],
        config_params["input_type_lat_long"],
        JOINER_REPLICA_COUNT,
        input_diff_name=str(config_params["replica_id"]),
    )

    vuelos_communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"]
    )
    vuelos_receiver = vuelos_communication_initializer.initialize_receiver(
        config_params["vuelos_input"],
        config_params["input_type_vuelos"],
        config_params["replicas_count"],
    )
    vuelos_sender = vuelos_communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    joiner = Joiner(lat_long_receiver, vuelos_receiver, vuelos_sender)
    joiner.run()


if __name__ == "__main__":
    main()
