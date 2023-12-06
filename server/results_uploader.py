import logging
import signal

from commons.protocol import ResultMessage


class ResultsUploader:
    """
    It sends the results to the client through a socket
    """

    def __init__(self, receiver, buff, ack_results_queue):
        self.receiver = receiver
        self.buff = buff
        self.ack_results_queue = ack_results_queue

    def start(self):
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)
        logging.info(f"action: results_uploader | result: started")
        self.receiver.bind(self.output_callback, self.handle_eof)
        self.receiver.start()

    def output_callback(self, messages):
        message_batch = "\n".join(messages.payload)
        self.output_single(message_batch)

    def output_single(self, content):
        try:
            message = ResultMessage(content)
            self.buff.send_message(message)
            self.ack_results_queue.get()
            logging.debug(f"action: result_upload | result: success")
        except OSError as e:
            logging.error(f"action: result_upload | result: fail | error: {e}")

    def handle_eof(self):
        # TODO: handle
        pass

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: results_uploader_shutdown | result: in_progress")
        self.receiver.stop()
        self.buff.stop()
        logging.info("action: results_uploader_shutdown | result: success")
