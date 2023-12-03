from multiprocessing import Process

from health_checker import HealthChecker, HealthCheckerConfig
from commons.health_checker_server import HealthCheckerServer
from commons.log_initializer import initialize_log
from commons.config_initializer import initialize_config


def main():
    config_inputs = {
        "logging_level": str,
        "replica_id": int,
        "filter_general_replicas": int,
        "filter_multiple_replicas": int,
        "filter_avg_max_replicas": int,
        "filter_distancia_replicas": int,
        "filter_tres_escalas_o_mas_replicas": int,
        "filter_dos_mas_rapidos_replicas": int,
        "filter_lat_long_replicas": int,
        "processor_tres_escalas_o_mas_replicas": int,
        "processor_dos_mas_rapidos_replicas": int,
        "processor_distancias_replicas": int,
        "processor_max_avg_replicas": int,
        "processor_media_general_replicas": int,
        "tagger_dos_mas_rapidos_replicas": int,
        "tagger_tres_escalas_o_mas_replicas": int,
        "tagger_distancias_replicas": int,
        "tagger_max_avg_replicas": int,
        "load_balancer_replicas": int,
        "grouper_replicas": int,
        "joiner_replicas": int,
        "server_replicas": int,
        "health_checker_replicas": int
    }
    config_params = initialize_config(config_inputs)

    logging_level = config_params["logging_level"]
    initialize_log(logging_level)

    # Healthcheck process
    health = Process(target=HealthCheckerServer().run)
    health.start()

    config = HealthCheckerConfig(
        config_params["replica_id"],
        config_params["filter_general_replicas"],
        config_params["filter_multiple_replicas"],
        config_params["filter_avg_max_replicas"],
        config_params["filter_distancia_replicas"],
        config_params["filter_tres_escalas_o_mas_replicas"],
        config_params["filter_dos_mas_rapidos_replicas"],
        config_params["filter_lat_long_replicas"],
        config_params["processor_tres_escalas_o_mas_replicas"],
        config_params["processor_dos_mas_rapidos_replicas"],
        config_params["processor_distancias_replicas"],
        config_params["processor_max_avg_replicas"],
        config_params["processor_media_general_replicas"],
        config_params["tagger_dos_mas_rapidos_replicas"],
        config_params["tagger_tres_escalas_o_mas_replicas"],
        config_params["tagger_distancias_replicas"],
        config_params["tagger_max_avg_replicas"],
        config_params["load_balancer_replicas"],
        config_params["grouper_replicas"],
        config_params["joiner_replicas"],
        config_params["server_replicas"],
        config_params["health_checker_replicas"]
    )

    HealthChecker(config).run()

    health.join()


if __name__ == "__main__":
    main()
