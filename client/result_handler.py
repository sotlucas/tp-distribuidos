import datetime
import logging
import os
import signal

from commons.protocol import PeerDisconnected


class ResultHandler:
    def __init__(self, buff, replica_id):
        self.tstamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.running = True
        self.buff = buff
        self.replica_id = replica_id

    def receive_results(self):
        """
        Receive the results from the server.
        """
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

        logging.info("Receiving results")
        while self.running:
            try:
                message = self.buff.get_message()
                if not message:
                    break
                logging.debug(f"Result received: {message.type} | {message.content}")
                self.save_results(message.content.decode())
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

    def save_results(self, data):
        """
        Saves the results in the corresponding file.
        """
        results = data.split("\n")
        for result in results:
            self.save_result_single(result)

    def save_result_single(self, data):
        # get the tag between [] to identify the file
        logging.info(f"Saving result: {data}")
        file_name = data.split("[")[1].split("]")[0].lower()

        if not os.path.exists(f"results/client_{self.replica_id}"):
            os.makedirs(f"results/client_{self.replica_id}")

        with open(f"results/client_{self.replica_id}/{self.tstamp}_{file_name}.txt", "a") as f:
            f.write(data + "\n")

    def __stop(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: result_handler_shutdown | result: in_progress")
        self.running = False
        logging.info("action: result_handler_shutdown | result: success")
