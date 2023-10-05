from lat_long_uploader import LatLongUploader, LatLongUploaderConfig
from commons.communication import CommunicationConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "file_path": str,
        "remove_file_header": bool,
        "output_queue": str,
        "rabbit_host": str,
        "output_type": str,
        "logging_level": str,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    lat_long_uploader_config = LatLongUploaderConfig(
        config_params["file_path"], config_params["remove_file_header"]
    )
    communication_config = CommunicationConfig(
        None,
        config_params["output_queue"],
        config_params["rabbit_host"],
        None,
        config_params["output_type"],
    )
    LatLongUploader(lat_long_uploader_config, communication_config).run()


if __name__ == "__main__":
    main()
