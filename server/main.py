from server import Server, ServerConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import initialize_receiver, initialize_sender

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

    server_receiver = initialize_receiver(
        config_params["rabbit_host"],
        config_params["input"],
        SERVER_REPLICAS_COUNT,
        config_params["input_type"],
    )
    server_sender = initialize_sender(
        config_params["rabbit_host"],
        config_params["output"],
        config_params["output_type"],
    )

    server_config = ServerConfig(
        config_params["server_port"],
        config_params["connection_timeout"],
    )
    Server(server_config, server_receiver, server_sender).run()


if __name__ == "__main__":
    main()
