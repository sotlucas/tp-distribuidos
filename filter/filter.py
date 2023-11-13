import logging
import signal


class FilterConfig:
    def __init__(self, input_fields, output_fields, delimiter):
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.delimiter = delimiter


class Filter:
    def __init__(self, config, communication_receiver, communication_sender):
        self.config = config
        self.communication_receiver = communication_receiver
        self.communication_sender = communication_sender
        self.input_fields = config.input_fields.split(",")
        self.output_fields = config.output_fields.split(",")
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)

    def run(self):
        self.communication_receiver.bind(
            input_callback=self.filter,
            eof_callback=self.communication_sender.send_eof,
            sender=self.communication_sender,
            input_fields_order=self.input_fields,
        )
        self.communication_receiver.start()

    def filter(self, messages):
        self.communication_sender.send_all(
            list(map(lambda x: {'corr_id': x['corr_id'], **x['content']}, messages)),
            output_fields_order=['corr_id'] + self.output_fields
        )

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: filter_shutdown | result: in_progress")
        self.communication_receiver.stop()
        self.communication_sender.stop()
        logging.info("action: filter_shutdown | result: success")
