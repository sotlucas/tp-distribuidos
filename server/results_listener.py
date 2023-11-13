import logging
import signal
import multiprocessing as mp


class ResultsListener:
    """
    Listens for results from a queue and forwards the message to the corresponding client handler.
    """

    def __init__(self, receiver, queue):
        self.receiver = receiver
        self.queue = queue
        self.client_handlers_queues = {}
        # self.client_handler_registration = mp.Process(target=self.add_client_handler, args=(queue,))

    def start(self):
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)
        logging.info(f"action: results_listener | result: started")
        self.receiver.bind(self.output_callback, self.handle_eof)
        self.receiver.start()

    def output_callback(self, messages):
        """
        Sends the messages to the corresponding client handler.
        """
        logging.debug(f"COSO {messages}")
        for message in messages:  # TODO: batches?
            self.client_handlers_queues[message['corr_id']].put(message['content'])

    def add_client_handler(self):
        """
        Add a client handler to the list of handlers.
        """
        logging.debug("action: add_client_handler | result: started")
        while True:
            message = self.queue.get()
            self.client_handlers_queues[message[0]] = message[1]
            logging.debug("action: add_client_handler | result: success")

    def handle_eof(self):
        # TODO: handle
        pass

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: results_listener_shutdown | result: in_progress")
        self.receiver.stop()
        logging.info("action: results_listener_shutdown | result: success")
