import logging
import sys
from client import Client, ClientConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main(arcv):
    if len(arcv) != 2:
        logging.error("Invalid arguments, usage: python main.py <file_path>")
        return
    file_path = arcv[1]

    config_inputs = {
        "server_ip": str,
        "server_port": int,
        "logging_level": str,
        "remove_file_header": bool,
        "batch_size": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    config = ClientConfig(
        config_params["server_ip"],
        config_params["server_port"],
        file_path,
        config_params["remove_file_header"],
        config_params["batch_size"],
    )
    Client(config).run()


if __name__ == "__main__":
    main(sys.argv)
