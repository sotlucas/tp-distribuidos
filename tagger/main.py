from tagger import Tagger
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import initialize_receiver, initialize_sender


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
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    receiver = initialize_receiver(
        config_params["rabbit_host"],
        config_params["input"],
        config_params["replicas_count"],
        config_params["input_type"],
    )
    sender = initialize_sender(
        config_params["rabbit_host"],
        config_params["output"],
        config_params["output_type"],
    )

    Tagger(config_params["tag_name"], receiver, sender).run()


if __name__ == "__main__":
    main()
