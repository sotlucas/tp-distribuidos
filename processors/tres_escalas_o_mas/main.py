from processor import Processor
from communication import Communication, CommunicationConfig
import logging
import os


def initialize_config():
    config_params = {}
    try:
        config_params["input_queue"] = os.getenv("INPUT_QUEUE")
        config_params["output_queue"] = os.getenv("OUTPUT_QUEUE")
        config_params["logging_level"] = os.getenv("LOGGING_LEVEL")
        config_params["rabbit_host"] = os.getenv("RABBIT_HOST")
        config_params["output_type"] = os.getenv(
            "OUTPUT_TYPE",
        )
        config_params["input_type"] = os.getenv(
            "INPUT_TYPE",
        )

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
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

    communication_config = CommunicationConfig(
        config_params["input_queue"],
        config_params["output_queue"],
        config_params["rabbit_host"],
        config_params["input_type"],
        config_params["output_type"],
    )

    processor = Processor()
    Communication(communication_config).run(processor.proccess)


if __name__ == "__main__":
    main()
