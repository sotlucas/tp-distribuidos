import logging
import signal


class Tagger:
    def __init__(self, tag_name, receiver, sender):
        self.tag_name = tag_name
        self.receiver = receiver
        self.sender = sender
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)

    def run(self):
        self.receiver.bind(
            input_callback=self.tag_messages,
            eof_callback=self.handle_eof,
            sender=self.sender,
        )
        self.receiver.start()

    def tag_messages(self, messages):
        tagged_messages = [self.tag_message(message) for message in messages]
        self.sender.send_all(tagged_messages)

    def tag_message(self, message):
        """
        Adds the tag name to the beginning of the message and sends it to the output.
        """
        return f"[{self.tag_name}]{message}"

    def handle_eof(self):
        # TODO: See if we need to do anything with the EOF here.
        logging.info(f"action: tagger | result: complete")
        pass

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: tagger_shutdown | result: in_progress")
        self.receiver.stop()
        self.sender.stop()
        logging.info("action: tagger_shutdown | result: success")
