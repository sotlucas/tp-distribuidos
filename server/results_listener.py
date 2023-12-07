import logging
import signal


class ResultsListener:
    """
    Listens for results from the results queue and sends them to the specific topic.
    """

    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender

    def start(self):
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)
        logging.info(f"action: results_listener | result: started")
        self.receiver.bind(self.output_callback, self.handle_result_eof)
        self.receiver.start()

    def output_callback(self, messages):
        logging.debug(
            f"action: results_listener | client_id: {messages.client_id} | messages_id: {messages.message_id}"
        )
        self.sender.send_all(messages, routing_key=str(messages.client_id))

    def handle_result_eof(self, eof_result_message):
        logging.debug(
            f"received result eof | client_id: {eof_result_message.client_id}, tag_id: {eof_result_message.tag_id}, messages_sent: {eof_result_message.messages_sent}"
        )
        self.sender.send_special_result_eof(
            eof_result_message.client_id,
            eof_result_message.tag_id,
            routing_key=str(eof_result_message.client_id),
            messages_sent=eof_result_message.messages_sent,
        )

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: results_listener_shutdown | result: in_progress")
        self.receiver.stop()
        self.sender.stop()
        logging.info("action: results_listener_shutdown | result: success")
