import logging
import signal

from commons.protocol import ResultEOFMessage, ResultMessage


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
        self.receiver.bind(self.output_callback, self.handle_result_eof)
        self.receiver.start()

    def output_callback(self, messages):
        message_batch = "\n".join(messages.payload)
        self.output_single(
            messages.tag_id,
            messages.message_id,
            message_batch,
        )

    def output_single(
        self,
        tag_id,
        message_id,
        content,
    ):
        try:
            message = ResultMessage(tag_id, message_id, content)
            self.buff.send_message(message)
            self.ack_results_queue.get()
            logging.debug(f"action: result_upload | result: success")
        except OSError as e:
            logging.error(f"action: result_upload | result: fail | error: {e}")

    def handle_result_eof(self, eof_result_message):
        try:
            # We convert the message to a ResultEOFMessage to be able to send it to the buffer
            logging.debug(
                f"action: eof_result_upload | tag_id: {eof_result_message.tag_id} | messages_sent: {eof_result_message.messages_sent}"
            )
            message = ResultEOFMessage(
                eof_result_message.tag_id, eof_result_message.messages_sent
            )
            self.buff.send_message(message)
            self.ack_results_queue.get()
            logging.debug(f"action: eof_result_upload | result: success")
        except OSError as e:
            logging.error(f"action: eof_result_upload | result: fail | error: {e}")

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: results_uploader_shutdown | result: in_progress")
        self.receiver.stop()
        logging.info("action: results_uploader_shutdown | result: success")
