import logging
import sys
from client import Client


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
    initialize_log(logging.DEBUG)

    # TODO: Change arguments to ENV variables
    if len(arcv) != 4:
        logging.error(
            "Invalid arguments, usage: python main.py <server_ip> <server_port> <file_path>"
        )
        return

    Client(arcv[1], int(arcv[2]), arcv[3]).run()


if __name__ == "__main__":
    main(sys.argv)
