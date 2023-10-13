from server import Server, ServerConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer

SERVER_REPLICAS_COUNT = 1


def main():
    config_inputs = {
        "server_port": int,
        "logging_level": str,
        "connection_timeout": int,
        "input": str,
        "output": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    vuelos_initializer = CommunicationInitializer(config_params["rabbit_host"])
    vuelos_receiver = vuelos_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        SERVER_REPLICAS_COUNT,
    )

    resultados_initializer = CommunicationInitializer(config_params["rabbit_host"])
    resultados_sender = resultados_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    server_config = ServerConfig(
        config_params["server_port"],
        config_params["connection_timeout"],
    )
    Server(server_config, vuelos_receiver, resultados_sender).run()


if __name__ == "__main__":
    main()
