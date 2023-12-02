from multiprocessing import Process

from commons.health_checker import HealthChecker
from load_balancer import LoadBalancer, LoadBalancerConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from commons.connection import ConnectionConfig, Connection
from commons.restorer import Restorer
from commons.log_storer import LogStorer


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "replicas_count": int,
        "grouper_replicas_count": int,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthChecker().run)
    health.start()

    restore_state = Restorer().restore()

    communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"],
    )
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        config_params["replica_id"],
        config_params["replicas_count"],
        messages_received_restore_state=restore_state.get_messages_received(),
        possible_duplicates_restore_state=restore_state.get_possible_duplicates(),
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"],
        config_params["output_type"],
        messages_sent_restore_state=restore_state.get_messages_sent(),
    )

    input_fields = [
        "startingAirport",
        "destinationAirport",
        "totalFare",
    ]
    output_fields = input_fields

    load_balancer_config = LoadBalancerConfig(config_params["grouper_replicas_count"])

    connection_config = ConnectionConfig(
        config_params["replica_id"], input_fields, output_fields, is_topic=True
    )
    Connection(
        connection_config, receiver, sender, LoadBalancer, load_balancer_config
    ).run()

    health.join()


if __name__ == "__main__":
    main()
