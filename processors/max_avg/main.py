from multiprocessing import Process

from commons.health_checker import HealthChecker
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer
from max_avg import MaxAvg
from commons.connection import ConnectionConfig, Connection


def main():
    config_inputs = {
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "replicas_count": int,
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
        config_params["replicas_count"],
    )
    sender = communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    input_fields = ["route", "prices"]
    output_fields = ["route", "avg", "max_price"]

    connection_config = ConnectionConfig(input_fields, output_fields)
    Connection(connection_config, receiver, sender, MaxAvg).run()

    health.join()


if __name__ == "__main__":
    main()
