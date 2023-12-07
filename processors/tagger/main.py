from multiprocessing import Process

from commons.health_checker_server import HealthCheckerServer
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from tagger import Tagger, TaggerConfig
from commons.connection import ConnectionConfig, Connection
from commons.log_guardian import LogGuardian


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "replicas_count": int,
        "tag_name": str,
        "tag_id": int,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthCheckerServer().run)
    health.start()

    log_guardian = LogGuardian()

    communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"], log_guardian
    )
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        config_params["replica_id"],
        config_params["replicas_count"],
        use_duplicate_catcher=True,
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"],
        config_params["output_type"],
    )

    tagger_config = TaggerConfig(config_params["tag_name"])

    # We don't need to specify input and output fields for the Tagger
    # TODO: We do not need to send EOF now, but maybe we will need it when we handle multiple clients
    connection_config = ConnectionConfig(
        config_params["replica_id"],
        result_tag_id=config_params["tag_id"],
    )
    Connection(
        connection_config, receiver, sender, log_guardian, Tagger, tagger_config
    ).run()

    health.join()


if __name__ == "__main__":
    main()
