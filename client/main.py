import logging
import sys
import os
from client import Client, ClientConfig
from configparser import ConfigParser


def initialize_config():
    """Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["server_ip"] = os.getenv(
            "SERVER_IP", config["DEFAULT"]["SERVER_IP"]
        )
        config_params["server_port"] = int(
            os.getenv("SERVER_PORT", config["DEFAULT"]["SERVER_PORT"])
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["remove_file_header"] = bool(
            os.getenv("REMOVE_FILE_HEADER", config["DEFAULT"]["REMOVE_FILE_HEADER"])
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e)
        )
    return config_params


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main(arcv):
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    # TODO: Change arguments to ENV variables
    if len(arcv) != 2:
        logging.error("Invalid arguments, usage: python main.py <file_path>")
        return

    file_path = arcv[1]
    config = ClientConfig(
        config_params["server_ip"],
        config_params["server_port"],
        file_path,
        config_params["remove_file_header"],
    )
    Client(config).run()


if __name__ == "__main__":
    main(sys.argv)
