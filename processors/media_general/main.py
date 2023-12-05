from multiprocessing import Process

from commons.health_checker_server import HealthCheckerServer
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from media_general import MediaGeneral, MediaGeneralConfig
from commons.connection import ConnectionConfig, Connection
from commons.restorer import Restorer
from commons.log_guardian import LogGuardian


MEDIA_GENERAL_REPLICAS_COUNT = 1


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "grouper_replicas_count": int,
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
        MEDIA_GENERAL_REPLICAS_COUNT,
        use_duplicate_catcher=True,
        use_duplicate_catcher=True,
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    input_fields = ["sum", "amount"]
    output_fields = ["media_general"]

    media_general_config = MediaGeneralConfig(config_params["grouper_replicas_count"])

    connection_config = ConnectionConfig(
        config_params["replica_id"],
        input_fields,
        output_fields,
        has_statefull_processor=True,
    )
    Connection(
        connection_config,
        receiver,
        sender,
        log_guardian,
        MediaGeneral,
        media_general_config,
    ).run()

    health.join()


if __name__ == "__main__":
    main()
