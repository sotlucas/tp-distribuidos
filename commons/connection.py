import logging
import signal
from commons.protocol import EOF


class ConnectionConfig:
    def __init__(
        self, input_fields=None, output_fields=None, send_eof=True, is_topic=False
    ):
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.send_eof = send_eof
        self.is_topic = is_topic


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

        if self.config.is_topic:
            self.send_messages_topic(processed_messages)
        else:
            self.send_messages(processed_messages)

    def send_messages_topic(self, messages):
        # message: (topic, message)
        messages_by_topic = {}
        for message in messages:
            messages_by_topic[message[0]] = messages_by_topic.get(message[0], []) + [
                message[1]
            ]
        for topic, messages in messages_by_topic.items():
            self.communication_sender.send_all(
                messages,
                routing_key=str(topic),
                output_fields_order=self.config.output_fields,
            )

    def send_messages(self, messages):
        self.communication_sender.send_all(
            messages, output_fields_order=self.config.output_fields
        )

    def handle_eof(self):
        messages = self.processor.finish_processing()
        if messages is EOF:
            # If the processor returns EOF, it means that it wants to stop the connection.
            # This is useful for the joiner, which needs to stop the connection when it finishes processing.
            # TODO: See if this can be changed
            self.__shutdown()
            return

        if self.config.is_topic:
            # TODO: If needed, we should move the topic name the finish_processing of the processor.
            TOPIC_EOF = "1"
            self.communication_sender.send_eof(TOPIC_EOF)
            return
        if messages:
            self.send_messages(messages)
        if self.config.send_eof:
            self.communication_sender.send_eof()

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: shutdown | result: in_progress")
        # TODO: neccesary to call finish_processing?
        # self.processor.finish_processing()
        if self.communication_receiver:
            self.communication_receiver.stop()
        if self.communication_sender:
            self.communication_sender.stop()
        logging.info("action: shutdown | result: success")
