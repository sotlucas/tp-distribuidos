from multiprocessing import Process

from commons.health_checker import HealthChecker
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from dos_mas_rapidos import DosMasRapidos
from commons.connection import ConnectionConfig, Connection
from commons.restorer import Restorer
from commons.log_storer import LogStorer


# TODO: Ver si se pueden replicar de alguna manera este processor
DOS_MAS_RAPIDOS_REPLICAS_COUNT = 1


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
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
        DOS_MAS_RAPIDOS_REPLICAS_COUNT,
        restore_state=restore_state,
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"],
        config_params["output_type"],
        restore_state=restore_state,
    )

    input_output_fields = [
        "legId",
        "startingAirport",
        "destinationAirport",
        "travelDuration",
        "segmentsArrivalAirportCode",
    ]

    connection_config = ConnectionConfig(
        config_params["replica_id"],
        input_output_fields,
        input_output_fields,
        duplicate_catcher=True,
    )
    Connection(
        connection_config,
        receiver,
        sender,
        DosMasRapidos,
        log_storer=log_storer,
        duplicate_catcher_restore_state=restore_state.get_duplicate_catcher_state(),
    ).run()

    health.join()


if __name__ == "__main__":
    main()
