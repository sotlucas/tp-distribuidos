import datetime
import logging
import multiprocessing
import os
import signal

from commons.communication_buffer import PeerDisconnected
from commons.protocol import MessageType


class ResultHandler:
    def __init__(self, client_id, results_queue):
        self.tstamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.running = True
        self.client_id = client_id
        self.results_queue = results_queue

        self.results_received = {}

        # We need to keep track of the EOFs received to know when to stop the result handler
        # {tag_id: messages_sent}
        self.eofs_received = {}

        self.create_save_dir()

    def create_save_dir(self):
        """
        Creates the directory to save the results.
        """
        if not os.path.exists(f"results/client_{self.client_id}/temp"):
            os.makedirs(f"results/client_{self.client_id}/temp")
        else:
            # Remove previous temp files
            for file in os.listdir(f"results/client_{self.client_id}/temp"):
                os.remove(f"results/client_{self.client_id}/temp/{file}")

    def receive_results(self):
        """
        Receive the results from the server.
        """
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

        processes = multiprocessing.cpu_count()
        workers_queue = multiprocessing.Queue(maxsize=processes)

        # start processes
        workers = []
        for _ in range(processes):
            result_saver = ResultSaver(self.client_id, workers_queue)
            p = multiprocessing.Process(target=result_saver.run)
            workers.append(p)
            p.start()

        logging.info("Receiving results")
        while self.running:
            try:
                message = self.results_queue.get()
                if not message:
                    break

                if message.message_type == MessageType.RESULT_EOF:
                    logging.info(
                        f"action: eof_result_received, tag_id: {message.tag_id}, messages_sent: {message.messages_sent}"
                    )
                    self.eofs_received[message.tag_id] = message.messages_sent
                else:
                    if not self.is_duplicate(message):
                        workers_queue.put((message, False))

                self.check_if_all_results_received()
            except PeerDisconnected:
                logging.info("action: server_disconected")
                self.running = False
            except OSError as e:
                # When receiving SIGTERM, the socket is closed and a OSError is raised.
                # If not we want to raise the exception.
                if self.running:
                    raise e
                return

        # Send shutdown signal to workers
        logging.info("Sending shutdown signal to workers")
        for _ in range(processes):
            workers_queue.put((None, None))

        logging.info("Waiting for workers to finish")
        # Wait for workers to finish
        for p in workers:
            p.join()
        logging.info("Workers finished")

        self.merge_results()

        logging.info("All results received!")

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

    def check_if_all_results_received(self):
        """
        Checks if all the results have been received.
        """
        QUERY_NUMBER = 4
        if len(self.eofs_received) < QUERY_NUMBER:
            return
        eof_finished = 0
        for tag_id, messages_sent in self.eofs_received.items():
            if len(self.results_received[tag_id]) == messages_sent:
                eof_finished += 1
        if eof_finished == QUERY_NUMBER:
            logging.info("action: result_handler | result: all_results_received")
            self.running = False

    def merge_results(self):
        """
        Merges all the results in a single file for each tag.
        """
        logging.info("Merging results")
        workers = []
        for tag_id in self.results_received:
            # Create a new process to merge the results for each tag
            p = multiprocessing.Process(
                target=self.merge_results_for_tag, args=(tag_id,)
            )
            workers.append(p)
            p.start()

        # Wait for workers to finish
        for p in workers:
            p.join()
        logging.info("Merge workers finished")

    def merge_results_for_tag(self, tag_id):
        """
        Merges all the results for a single tag.
        """
        logging.info(f"Merging results for tag {tag_id}")

        tag_name = self.get_tag_name_by_id(tag_id)
        file_name = f"results/client_{self.client_id}/{self.tstamp}_{tag_name}.txt"
        with open(file_name, "w") as merged_file:
            for file in os.listdir(f"results/client_{self.client_id}/temp"):
                if file.startswith(tag_name):
                    with open(f"results/client_{self.client_id}/temp/{file}") as f:
                        for line in f:
                            merged_file.write(line)
                        merged_file.write("\n")

        self.remove_temp_files_for_tag(tag_name)

    def remove_temp_files_for_tag(self, tag_name):
        """
        Removes the temp files for a single tag.
        """
        for file in os.listdir(f"results/client_{self.client_id}/temp"):
            if file.startswith(tag_name):
                os.remove(f"results/client_{self.client_id}/temp/{file}")

    def get_tag_name_by_id(self, tag_id):
        """
        Gets the tag name by the tag id.
        """
        if tag_id == 1:
            return "dos_mas_rapidos"
        elif tag_id == 2:
            return "tres_escalas"
        elif tag_id == 3:
            return "distancias"
        elif tag_id == 4:
            return "max_avg"

    def __stop(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: result_handler_shutdown | result: in_progress")
        self.running = False
        logging.info("action: result_handler_shutdown | result: success")


class ResultSaver:
    def __init__(self, client_id, results_queue):
        self.running = True
        self.client_id = client_id
        self.results_queue = results_queue

    def run(self):
        while True:
            result_message, end = self.results_queue.get()
            if not result_message and not end:
                logging.info("action: result_saver | result: shutdown")
                return
            self.save_temp_results(result_message)

    def save_temp_results(self, result_message):
        """
        Saves the temp results in the corresponding file.
        """
        result_id = result_message.message_id
        message_tag = self.get_message_tag(result_message.result)

        file_name = (
            f"results/client_{self.client_id}/temp/{message_tag}_{result_id}.txt"
        )
        with open(file_name, "a") as f:
            f.write(result_message.result)

    def get_message_tag(self, data):
        """
        Gets the tag between [] to identify the file.
        """
        return data.split("[")[1].split("]")[0].lower()
