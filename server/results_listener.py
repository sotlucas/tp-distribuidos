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
        self.receiver.bind(self.output_callback, self.handle_eof)
        self.receiver.start()

    def output_callback(self, messages):
        logging.debug(f"action: results_listener | messages: {messages.payload}")
        self.sender.send_all(messages, routing_key=str(messages.client_id))

    def handle_eof(self):
        # TODO: handle
        pass

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: results_listener_shutdown | result: in_progress")
        self.receiver.stop()
        self.sender.stop()
        logging.info("action: results_listener_shutdown | result: success")
