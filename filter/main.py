from filter import Filter, FilterConfig
from commons.communication_initializer import initialize_receiver, initialize_sender
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "input_fields": str,
        "output_fields": str,
        "input": str,
        "output": str,
        "logging_level": str,
        "rabbit_host": str,
        "output_type": str,
        "input_type": str,
        "delimiter": str,
        "replicas_count": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    communication_receiver = initialize_receiver(
        config_params["rabbit_host"],
        config_params["input"],
        config_params["replicas_count"],
        config_params["input_type"],
        output=config_params["output"],
    )
    communication_sender = initialize_sender(
        config_params["rabbit_host"],
        config_params["output"],
        config_params["output_type"],
    )

    filter_config = FilterConfig(
        config_params["input_fields"],
        config_params["output_fields"],
        config_params["delimiter"],
    )
    Filter(filter_config, communication_receiver, communication_sender).run()


if __name__ == "__main__":
    main()
