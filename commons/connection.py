import logging
import signal


class ConnectionConfig:
    def __init__(self, input_fields=None, output_fields=None, send_eof=True):
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.send_eof = send_eof


class Connection:
    def __init__(self, config, communication_receiver, communication_sender, processor):
        self.config = config
        self.communication_receiver = communication_receiver
        self.communication_sender = communication_sender
        self.processor = processor
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)

    def run(self):
        self.communication_receiver.bind(
            input_callback=self.process,
            eof_callback=self.handle_eof,
            sender=self.communication_sender,
            input_fields_order=self.config.input_fields,
        )
        self.communication_receiver.start()

    def process(self, messages):
        processed_messages = []
        for message in messages:
            processed_message = self.processor.process(message)
            if processed_message:
                processed_messages.append(processed_message)
        self.communication_sender.send_all(
            processed_messages, output_fields_order=self.config.output_fields
        )

    def handle_eof(self):
        messages = self.processor.finish_processing()
        if messages:
            self.communication_sender.send_all(
                messages, output_fields_order=self.config.output_fields
            )
        if self.config.send_eof:
            self.communication_sender.send_eof()

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: shutdown | result: in_progress")
        # TODO: neccesary to call finish_processing?
        # self.processor.finish_processing()
        self.communication_receiver.stop()
        self.communication_sender.stop()
        logging.info("action: shutdown | result: success")
