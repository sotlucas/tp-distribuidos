from lat_long_uploader import LatLongUploader, LatLongUploaderConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config
from commons.communication_initializer import CommunicationInitializer


def main():
    config_inputs = {
        "file_path": str,
        "remove_file_header": bool,
        "output": str,
        "rabbit_host": str,
        "output_type": str,
        "logging_level": str,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    communication_initializer = CommunicationInitializer(config_params["rabbit_host"])
    sender = communication_initializer.initialize_sender(
        config_params["output"], config_params["output_type"]
    )

    lat_long_uploader_config = LatLongUploaderConfig(
        config_params["file_path"], config_params["remove_file_header"]
    )
    LatLongUploader(lat_long_uploader_config, sender).run()


if __name__ == "__main__":
    main()
