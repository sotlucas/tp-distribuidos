from multiprocessing import Process

from commons.health_checker import HealthChecker
from server import Server, ServerConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from commons.restorer import Restorer
from commons.log_storer import LogStorer

SERVER_REPLICAS_COUNT = 1


def main():
    config_inputs = {
        "server_port": int,
        "logging_level": str,
        "connection_timeout": int,
        "vuelos_input": str,
        "vuelos_output": str,
        "lat_long_output": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "max_clients": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthChecker().run)
    health.start()

    # TODO: The server should not even have log storer or a state to restore, check this
    restore_state = Restorer().restore()

    vuelos_initializer = CommunicationInitializer(
        config_params["rabbit_host"],
    )

    resultados_initializer = CommunicationInitializer(
        config_params["rabbit_host"],
    )
    resultados_sender = resultados_initializer.initialize_sender(
        config_params["vuelos_output"],
        config_params["output_type"],
        messages_sent_restore_state=restore_state.get_messages_sent(),
    )

    lat_long_initializer = CommunicationInitializer(
        config_params["rabbit_host"],
    )
    lat_long_sender = lat_long_initializer.initialize_sender(
        config_params["lat_long_output"],
        config_params["output_type"],
        messages_sent_restore_state=restore_state.get_messages_sent(),
    )

    server_config = ServerConfig(
        config_params["server_port"],
        config_params["connection_timeout"],
        config_params["vuelos_input"],
        config_params["input_type"],
        config_params["max_clients"],
    )
    Server(server_config, vuelos_initializer, resultados_sender, lat_long_sender).run()


if __name__ == "__main__":
    main()
