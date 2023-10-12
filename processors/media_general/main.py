from processor import Processor
from commons.communication import Communication, CommunicationConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "input_queue": str,
        "output_queue": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "grouper_replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    communication_config = CommunicationConfig(
        config_params["input_queue"],
        config_params["output_queue"],
        config_params["rabbit_host"],
        config_params["input_type"],
        config_params["output_type"],
        1,
    )

    processor = Processor(
        config_params["grouper_replicas_count"], Communication(communication_config)
    )
    processor.run()


if __name__ == "__main__":
    main()
