import signal
import socket
import logging
import time
from multiprocessing import Process
import docker

from commons.communication_buffer import CommunicationBuffer, PeerDisconnected
from commons.protocol import HealthCheckMessage, MessageType

HEALTH_CHECKER_PORT = 5000
CONNECTION_RETRY_INTERVAL = 5
HEALTH_CHECK_INTERVAL = 20


class HealthCheckerConfig:
    def __init__(
        self,
        replica_id,
        filter_general_replicas,
        filter_multiple_replicas,
        filter_avg_max_replicas,
        filter_distancia_replicas,
        filter_tres_escalas_o_mas_replicas,
        filter_dos_mas_rapidos_replicas,
        filter_lat_long_replicas,
        processor_tres_escalas_o_mas_replicas,
        processor_dos_mas_rapidos_replicas,
        processor_distancias_replicas,
        processor_max_avg_replicas,
        processor_media_general_replicas,
        tagger_dos_mas_rapidos_replicas,
        tagger_tres_escalas_o_mas_replicas,
        tagger_distancias_replicas,
        tagger_max_avg_replicas,
        load_balancer_replicas,
        grouper_replicas,
        joiner_replicas,
        server_replicas,
        health_checker_replicas,
    ):
        self.replica_id = replica_id
        self.filter_general_replicas = filter_general_replicas
        self.filter_multiple_replicas = filter_multiple_replicas
        self.filter_avg_max_replicas = filter_avg_max_replicas
        self.filter_distancia_replicas = filter_distancia_replicas
        self.filter_tres_escalas_o_mas_replicas = filter_tres_escalas_o_mas_replicas
        self.filter_dos_mas_rapidos_replicas = filter_dos_mas_rapidos_replicas
        self.filter_lat_long_replicas = filter_lat_long_replicas
        self.processor_tres_escalas_o_mas_replicas = (
            processor_tres_escalas_o_mas_replicas
        )
        self.processor_dos_mas_rapidos_replicas = processor_dos_mas_rapidos_replicas
        self.processor_distancias_replicas = processor_distancias_replicas
        self.processor_max_avg_replicas = processor_max_avg_replicas
        self.processor_media_general_replicas = processor_media_general_replicas
        self.tagger_dos_mas_rapidos_replicas = tagger_dos_mas_rapidos_replicas
        self.tagger_tres_escalas_o_mas_replicas = tagger_tres_escalas_o_mas_replicas
        self.tagger_distancias_replicas = tagger_distancias_replicas
        self.tagger_max_avg_replicas = tagger_max_avg_replicas
        self.load_balancer_replicas = load_balancer_replicas
        self.grouper_replicas = grouper_replicas
        self.joiner_replicas = joiner_replicas
        self.server_replicas = server_replicas
        self.health_checker_replicas = health_checker_replicas


class HealthChecker:
    def __init__(self, config):
        self.config = config
        self.running = True
        # reduce log level for docker
        logging.getLogger("docker").setLevel(logging.WARNING)
        self.docker = docker.from_env()
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        """
        Starts the health checker.
        """
        # Call all the processors to check if they are healthy. Each in a new process.
        processor_checkers = []
        self.init_checker(
            "tp1-filter_general_",
            self.config.filter_general_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-filter_multiple_",
            self.config.filter_multiple_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-filter_avg_max_",
            self.config.filter_avg_max_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-filter_distancia_",
            self.config.filter_distancia_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-filter_tres_escalas_o_mas_",
            self.config.filter_tres_escalas_o_mas_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-filter_dos_mas_rapidos_",
            self.config.filter_dos_mas_rapidos_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-filter_lat_long_",
            self.config.filter_lat_long_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-processor_tres_escalas_o_mas_",
            self.config.processor_tres_escalas_o_mas_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-processor_dos_mas_rapidos_",
            self.config.processor_dos_mas_rapidos_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-processor_distancias_",
            self.config.processor_distancias_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-processor_max_avg_",
            self.config.processor_max_avg_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-processor_media_general_",
            self.config.processor_media_general_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-tagger_dos_mas_rapidos_",
            self.config.tagger_dos_mas_rapidos_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-tagger_tres_escalas_o_mas_",
            self.config.tagger_tres_escalas_o_mas_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-tagger_distancias_",
            self.config.tagger_distancias_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-tagger_max_avg_",
            self.config.tagger_max_avg_replicas,
            processor_checkers,
        )
        self.init_checker(
            "tp1-load_balancer_", self.config.load_balancer_replicas, processor_checkers
        )
        self.init_checker(
            "tp1-grouper_", self.config.grouper_replicas, processor_checkers
        )
        self.init_checker(
            "tp1-joiner_", self.config.joiner_replicas, processor_checkers
        )
        self.init_checker(
            "tp1-server_", self.config.server_replicas, processor_checkers
        )
        self.init_checker_hc(
            "tp1-health_checker_", self.config.replica_id, processor_checkers
        )

        # Wait for the processes to finish
        for processor_checker in processor_checkers:
            processor_checker.join()
            logging.info("Processor checker finished")

    def init_checker(
        self, processor_prefix, replicas, processor_checkers, processor_suffix="-1"
    ):
        """
        Connects to all the replicas of a processor.
        """
        for i in range(1, replicas + 1):
            processor_checker = Process(
                target=self.check_processor,
                args=(f"{processor_prefix}{i}{processor_suffix}",),
            )
            processor_checker.start()
            processor_checkers.append(processor_checker)

    def init_checker_hc(
        self,
        processor_prefix,
        current_replica_id,
        processor_checkers,
        processor_suffix="-1",
    ):
        """
        Connects to the next health checker replica.
        """
        next_replica_id = (current_replica_id % self.config.health_checker_replicas) + 1
        if next_replica_id == current_replica_id:
            # Prevent to connect to itself
            return
        processor_checker = Process(
            target=self.check_processor,
            args=(f"{processor_prefix}{next_replica_id}{processor_suffix}",),
        )
        processor_checker.start()
        processor_checkers.append(processor_checker)

    def check_processor(self, processor_name):
        """
        Checks if a processor is healthy.
        """
        while self.running:
            logging.info(f"Connecting to processor {processor_name}...")
            buff = self.connect_to_processor(processor_name)
            while self.running:
                try:
                    buff.send_message(HealthCheckMessage())
                    if buff.get_message().message_type == MessageType.HEALTH_OK:
                        logging.debug(f"HEALTHY :: {processor_name}")
                    time.sleep(HEALTH_CHECK_INTERVAL)
                except OSError as e:
                    logging.exception(f"Error: {e}")
                    return
                except PeerDisconnected:
                    self.restart_container(processor_name)
                    break

    def connect_to_processor(self, processor_name):
        """
        Connects to a processor.
        """
        # TODO: Waits for the whole system to start so that it doesn't try to restart all the containers.
        #       Change this to a more robust solution.
        time.sleep(HEALTH_CHECK_INTERVAL)
        while self.running:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(HEALTH_CHECK_INTERVAL)
                sock.connect((processor_name, HEALTH_CHECKER_PORT))
                buff = CommunicationBuffer(sock, timeout=HEALTH_CHECK_INTERVAL)
                break
            except Exception as e:
                logging.exception(f"Error: {e}")
                self.restart_container(processor_name)
                logging.warning(
                    f"Connection failed to processor {processor_name}. Retrying..."
                )
                time.sleep(HEALTH_CHECK_INTERVAL)
        return buff

    def restart_container(self, processor_name):
        """
        Restarts a container.
        """
        logging.error(f"UNHEALTHY :: {processor_name}")
        container = self.docker.containers.list(
            all=True, filters={"name": processor_name}
        )[0]
        logging.info(f"Starting {processor_name}: {container}")
        container.restart(timeout=0)

    def __stop(self, *args):
        """
        Stop server closing the server socket.
        """
        logging.info("action: server_shutdown | result: in_progress")
        self.running = False
        logging.info("action: server_socket_closed | result: success")
        logging.info("action: server_shutdown | result: success")
