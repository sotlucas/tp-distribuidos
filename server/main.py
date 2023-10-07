from server import Server, ServerConfig
from commons.communication import CommunicationConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "server_port": int,
        "logging_level": str,
        "connection_timeout": int,
        "input_queue": str,
        "output_queue": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    server_config = ServerConfig(
        config_params["server_port"],
        config_params["connection_timeout"],
    )

    communication_config = CommunicationConfig(
        config_params["input_queue"],
        config_params["output_queue"],
        config_params["rabbit_host"],
        config_params["input_type"],
        config_params["output_type"],
        1,
    )
    Server(server_config, communication_config).run()


if __name__ == "__main__":
    main()
