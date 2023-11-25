from health_checker import HealthChecker, HealthCheckerConfig
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "logging_level": str,
        "filter_general_replicas": int,
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    config = HealthCheckerConfig(
        config_params["filter_general_replicas"],
    )

    HealthChecker(config).run()


if __name__ == "__main__":
    main()
