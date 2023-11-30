from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from dos_mas_rapidos import DosMasRapidos
from commons.connection import ConnectionConfig, Connection

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

    communication_initializer = CommunicationInitializer(config_params["rabbit_host"])
    receiver = communication_initializer.initialize_receiver(
        config_params["input"],
        config_params["input_type"],
        config_params["replica_id"],
        DOS_MAS_RAPIDOS_REPLICAS_COUNT,
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    input_output_fields = [
        "legId",
        "startingAirport",
        "destinationAirport",
        "travelDuration",
        "segmentsArrivalAirportCode",
    ]

    connection_config = ConnectionConfig(input_output_fields, input_output_fields)
    Connection(
        connection_config,
        receiver,
        sender,
        DosMasRapidos,
    ).run()


if __name__ == "__main__":
    main()
