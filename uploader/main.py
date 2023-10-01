import logging
import os
from configparser import ConfigParser
import time
from uploader import Uploader, UploaderConfig
from communication import CommunicationConfig


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
        config_params["server_port"] = int(
            os.getenv("SERVER_PORT", config["DEFAULT"]["SERVER_PORT"])
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["connection_timeout"] = int(
            os.getenv("CONNECTION_TIMEOUT", config["DEFAULT"]["CONNECTION_TIMEOUT"])
        )
        config_params["output_queue"] = os.getenv(
            "OUTPUT_QUEUE", config["DEFAULT"]["OUTPUT_QUEUE"]
        )
        config_params["rabbit_host"] = os.getenv(
            "RABBIT_HOST", config["DEFAULT"]["RABBIT_HOST"]
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


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    uploader_config = UploaderConfig(
        config_params["server_port"],
        config_params["connection_timeout"],
    )

    communication_config = CommunicationConfig(
        None,
        config_params["output_queue"],
        config_params["rabbit_host"],
        None,
        None,
    )
    Uploader(uploader_config, communication_config).run()


if __name__ == "__main__":
    main()
