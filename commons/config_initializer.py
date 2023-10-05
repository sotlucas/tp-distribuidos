import os
from configparser import ConfigParser


def initialize_config(config_inputs):
    """Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters

    Args:
        - config_params (dict): Dictionary with default config parameters.
            - Keys are the parameter names and values are the type of the parameter (int, str, bool)
                - Example: `{"server_port": int, "logging_level": str, "connection_timeout": int}`
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        for key, value in config_inputs.items():
            config_params[key] = value(
                os.getenv(key.upper(), config["DEFAULT"][key.upper()])
            )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e)
        )
    return config_params
