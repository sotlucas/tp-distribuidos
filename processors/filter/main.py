from multiprocessing import Process

from commons.health_checker_server import HealthCheckerServer
from filter import Filter, FilterConfig
from commons.communication_initializer import CommunicationInitializer
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.connection import ConnectionConfig, Connection
from commons.restorer import Restorer
from commons.log_storer import LogStorer


def main():
    config_inputs = {
        "input_fields": str,
        "output_fields": str,
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "delimiter": str,
        "replicas_count": int,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthCheckerServer().run)
    health.start()

    restore_state = Restorer().restore()

    communication_initializer = CommunicationInitializer(config_params["rabbit_host"])
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        config_params["replica_id"],
        config_params["replicas_count"],
        input_diff_name=config_params["output"],
        delimiter=config_params["delimiter"],
        messages_received_restore_state=restore_state.get_messages_received(),
        possible_duplicates_restore_state=restore_state.get_possible_duplicates(),
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"],
        config_params["output_type"],
        config_params["delimiter"],
        messages_sent_restore_state=restore_state.get_messages_sent(),
    )

    input_fields = config_params["input_fields"].split(",")
    output_fields = config_params["output_fields"].split(",")

    filter_config = FilterConfig(output_fields)

    connection_config = ConnectionConfig(
        config_params["replica_id"],
        input_fields,
        output_fields,
    )
    Connection(connection_config, receiver, sender, Filter, filter_config).run()

    health.join()


if __name__ == "__main__":
    main()
