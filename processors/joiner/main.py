from multiprocessing import Process

from commons.health_checker import HealthChecker
from joiner import Joiner, JoinerConfig
from lat_long import LatLong, LatLongConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from commons.connection import ConnectionConfig, Connection
from state import State
from commons.restorer import Restorer
from commons.log_storer import LogStorer

import threading

# Because they are working as an exchange, they do not share the same as the other groupers
# TODO: See if this can be changed
JOINER_REPLICA_COUNT = 1


def main():
    config_inputs = {
        "replica_id": int,
        "lat_long_input": str,
        "vuelos_input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type_lat_long": str,
        "input_type_vuelos": str,
        "replicas_count": int,
        "replica_id": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthChecker().run)
    health.start()

    lat_long_log_storer = LogStorer(suffix="lat_long")
    lat_long_restore_state = Restorer(suffix="lat_long").restore()

    # TODO: When restoring we need that the lat longs send the airports to the state.
    state = State()

    lat_long_communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"], lat_long_log_storer
    )
    lat_long_receiver = lat_long_communication_initializer.initialize_receiver(
        config_params["lat_long_input"],
        config_params["input_type_lat_long"],
        config_params["replica_id"],
        JOINER_REPLICA_COUNT,
        input_diff_name=str(config_params["replica_id"]),
        restore_state=lat_long_restore_state,
    )

    lat_long_input_fields = ["AirportCode", "Latitude", "Longitude"]

    lat_long_config = LatLongConfig(state)

    connection_config = ConnectionConfig(
        config_params["replica_id"],
        lat_long_input_fields,
        None,
        send_eof=False,
        duplicate_catcher=True,
    )
    connection = Connection(
        connection_config,
        lat_long_receiver,
        None,
        LatLong,
        lat_long_config,
        lat_long_log_storer,
        duplicate_catcher_restore_state=lat_long_restore_state.get_duplicate_catcher_state(),
    )
    lat_long_thread = threading.Thread(target=connection.run)
    lat_long_thread.start()

    joiner_log_storer = LogStorer(suffix="joiner")
    joiner_restore_state = Restorer(suffix="joiner").restore()

    vuelos_communication_initializer = CommunicationInitializer(
        config_params["rabbit_host"], joiner_log_storer
    )
    vuelos_receiver = vuelos_communication_initializer.initialize_receiver(
        config_params["vuelos_input"],
        config_params["input_type_vuelos"],
        config_params["replica_id"],
        config_params["replicas_count"],
        restore_state=joiner_restore_state,
    )
    vuelos_sender = vuelos_communication_initializer.initialize_sender(
        config_params["output"],
        config_params["output_type"],
        restore_state=joiner_restore_state,
    )

    vuelos_input_fields = [
        "legId",
        "startingAirport",
        "destinationAirport",
        "totalTravelDistance",
    ]
    vuelos_output_fields = [
        "legId",
        "startingAirport",
        "destinationAirport",
        "totalTravelDistance",
        "startingLatitude",
        "startingLongitude",
        "destinationLatitude",
        "destinationLongitude",
    ]

    joiner_config = JoinerConfig(state)

    connection_config = ConnectionConfig(
        config_params["replica_id"], vuelos_input_fields, vuelos_output_fields
    )
    connection = Connection(
        connection_config, vuelos_receiver, vuelos_sender, Joiner, joiner_config
    )
    joiner_thread = threading.Thread(target=connection.run)
    joiner_thread.start()

    lat_long_thread.join()
    joiner_thread.join()
    health.join()


if __name__ == "__main__":
    main()
