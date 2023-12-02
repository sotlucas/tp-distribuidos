from multiprocessing import Process

from commons.health_checker import HealthChecker
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from tagger import Tagger, TaggerConfig
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
        "tag_name": str,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthChecker().run)
    health.start()

    log_storer = LogStorer()
    restore_state = Restorer().restore()

    communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"], log_storer
    )
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        config_params["replica_id"],
        config_params["replicas_count"],
        restore_state=restore_state,
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"],
        config_params["output_type"],
        restore_state=restore_state,
    )

    tagger_config = TaggerConfig(config_params["tag_name"])

    # We don't need to specify input and output fields for the Tagger
    # TODO: We do not need to send EOF now, but maybe we will need it when we handle multiple clients
    connection_config = ConnectionConfig(
        config_params["replica_id"],
        send_eof=False,
        duplicate_catcher=True,
    )
    Connection(
        connection_config,
        receiver,
        sender,
        Tagger,
        tagger_config,
        log_storer=log_storer,
        duplicate_catcher_restore_state=restore_state.get_duplicate_catcher_state(),
    ).run()

    health.join()


if __name__ == "__main__":
    main()
