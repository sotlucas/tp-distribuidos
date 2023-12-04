import logging
import signal
import time

from commons.protocol import ClientProtocolMessage, MessageType, EOFMessage


class FileUploader:
    def __init__(
        self,
        message_type,
        file_path,
        remove_file_header,
        batch_size,
        client_id,
        recv_queue,
        send_queue,
    ):
        self.message_type = message_type
        self.file_path = file_path
        self.remove_file_header = remove_file_header
        self.batch_size = batch_size
        self.client_id = client_id
        self.current_message_id = 1
        self.recv_queue = recv_queue
        self.send_queue = send_queue

    def start(self):
        """
        Send the csv file line by line to the server.

        Each line represents a flight with all the columns separated by commas.
        """
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

        logging.info(f"Sending file: {self.file_path}")
        for batch in self.__next_batch(self.file_path, self.batch_size):
            if batch:
                message = ClientProtocolMessage(
                    self.current_message_id, self.message_type, batch
                )
                self.send_queue.put(message)
                message_recv = self.recv_queue.get()
                logging.info(
                    f"FILE_UPLOADER::{self.message_type} | message_recv: {message_recv}"
                )
                while message_recv.message_type != MessageType.ACK:
                    logging.info(f"Retrying message: {message}")
                    self.send_queue.put(message)
                    message_recv = self.recv_queue.get()
                    time.sleep(10)
                self.current_message_id += 1
        # Send message to indicate that the file has ended
        message = EOFMessage(self.message_type)
        self.send_queue.put(message)
        logging.info(f"File sent: {self.file_path}")

    def __next_batch(self, file_path, batch_size):
        """
        Gets a batch of rows from the file.
        """
        batch = []
        with open(file_path, "r") as f:
            if self.remove_file_header:
                # Skip the header
                next(f)
            for line in f:
                batch.append(line)
                if len(batch) == batch_size or not line:
                    yield "".join(batch)
                    batch = []
            yield "".join(batch)

    def __stop(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: file_uploader_shutdown | result: in_progress")
        logging.info("action: file_uploader_shutdown | result: success")
