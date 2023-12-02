from multiprocessing import Process

from commons.health_checker import HealthChecker
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from tres_escalas_o_mas import TresEscalasOMas
from commons.connection import Connection, ConnectionConfig


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "replicas_count": int,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthChecker().run)
    health.start()

    communication_initializer = CommunicationInitializer(config_params["rabbit_host"])
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        config_params["replica_id"],
        config_params["replicas_count"],
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    input_fields = [
        "legId",
        "startingAirport",
        "destinationAirport",
        "totalFare",
        "travelDuration",
        "segmentsArrivalAirportCode",
    ]
    output_fields = input_fields

    config = ConnectionConfig(config_params["replica_id"], input_fields, output_fields)
    Connection(config, receiver, sender, TresEscalasOMas).run()

    health.join()


if __name__ == "__main__":
    main()
