import datetime
import logging
import os
import signal

from commons.communication_buffer import PeerDisconnected


class ResultHandler:
    def __init__(self, client_id, results_queue):
        self.tstamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.running = True
        self.client_id = client_id
        self.results_queue = results_queue

        self.results_received = {}

    def receive_results(self):
        """
        Receive the results from the server.
        """
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

        logging.info("Receiving results")
        while self.running:
            try:
                message = self.results_queue.get()
                if not message:
                    break
                if not self.is_duplicate(message):
                    self.__save_results(message.result)
            except PeerDisconnected:
                logging.info("action: server_disconected")
                self.running = False
            except OSError as e:
                # When receiving SIGTERM, the socket is closed and a OSError is raised.
                # If not we want to raise the exception.
                if self.running:
                    raise e
                return

        logging.info("Results received")

    def is_duplicate(self, message):
        """
        Checks if the message is a duplicate. If so, it is not saved.
        """
        tag_id = message.tag_id
        message_id = message.message_id
        if tag_id not in self.results_received:
            self.results_received[tag_id] = set()
        if message_id in self.results_received[tag_id]:
            logging.warning(
                f"action: result_handler | result: duplicate | tag_id: {tag_id} | message_id: {message_id}"
            )
            return True
        self.results_received[tag_id].add(message_id)
        return False

    def __save_results(self, data):
        """
        Saves the results in the corresponding file.
        """
        results = data.split("\n")
        for result in results:
            self.__save_result_single(result)

    def __save_result_single(self, data):
        """
        Saves a single result in the corresponding file.
        """
        file_name = self.__get_message_tag(data)

        if not os.path.exists(f"results/client_{self.client_id}"):
            os.makedirs(f"results/client_{self.client_id}")

        with open(
            f"results/client_{self.client_id}/{self.tstamp}_{file_name}.txt", "a"
        ) as f:
            f.write(data + "\n")

    def __get_message_tag(self, data):
        """
        Gets the tag between [] to identify the file.
        """
        return data.split("[")[1].split("]")[0].lower()

    def __stop(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: result_handler_shutdown | result: in_progress")
        self.running = False
        logging.info("action: result_handler_shutdown | result: success")
