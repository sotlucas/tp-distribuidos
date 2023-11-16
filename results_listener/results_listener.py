import logging
import signal


class ResultsListener:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.send_eof_wrapper,
            sender=self.sender,
        )
        self.receiver.start()

    def process(self, messages):
        logging.info(f"action: results_listener | result: received_messages | messages: {messages}")
        processed_messages = {}  # {queue_id: [messages]}
        for message in messages:
            message, queue_id = message['content'], message['corr_id']
            if queue_id not in processed_messages:
                processed_messages[queue_id] = []
            processed_messages[queue_id].append(message)
        for queue_id, messages_to_send in processed_messages.items():
            self.sender.send_all(
                messages_to_send,
                str(queue_id),
            )

    def send_eof_wrapper(self):
        # TODO: check
        pass

    def __stop(self, *args):
        """
        Shutdown. Closing connections.
        """
        logging.info("action: processor_shutdown | result: in_progress")
        self.receiver.stop()
        self.sender.stop()
        logging.info("action: processor_shutdown | result: success")
