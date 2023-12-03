import logging
import signal
from commons.message import ProtocolMessage
from commons.processor import ResponseType
from commons.duplicate_catcher import DuplicateCatcher


class ConnectionConfig:
    def __init__(
        self,
        replica_id,
        input_fields=None,
        output_fields=None,
        send_eof=True,
        is_topic=False,
        duplicate_catcher=False,
    ):
        self.replica_id = replica_id
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.send_eof = send_eof
        self.is_topic = is_topic
        self.duplicate_catcher = duplicate_catcher


class Connection:
    def __init__(
        self,
        config,
        communication_receiver,
        communication_sender,
        log_guardian,
        processor_name,
        processor_config=None,
    ):
        self.config = config
        self.communication_receiver = communication_receiver
        self.communication_sender = communication_sender
        self.processor_name = processor_name
        self.processor_config = processor_config
        self.log_guardian = log_guardian

        self.processors = {}

        self.duplicate_catchers = {}
        # Restore duplicate catcher state
        for (
            client_id,
            duplicate_catcher_state,
        ) in log_guardian.get_duplicate_catchers().items():
            duplicate_catcher = DuplicateCatcher(duplicate_catcher_state)
            self.duplicate_catchers[client_id] = duplicate_catcher

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

    def get_processor(self, client_id):
        if client_id not in self.processors:
            processor = (
                self.processor_name(self.processor_config, client_id)
                if self.processor_config
                else self.processor_name(client_id)
            )
            self.processors[client_id] = processor
        return self.processors[client_id]

    def get_duplicate_catcher(self, client_id):
        if client_id not in self.duplicate_catchers:
            duplicate_catcher = DuplicateCatcher()
            self.duplicate_catchers[client_id] = duplicate_catcher
        return self.duplicate_catchers[client_id]

    def process(self, messages):
        if self.config.duplicate_catcher:
            if self.process_duplicate_catcher(messages):
                # It means that the message is a duplicate
                return

        processor = self.get_processor(messages.client_id)
        processed_messages = []
        for message in messages.payload:
            processed_message = processor.process(message)
            if processed_message:
                if processed_message.type == ResponseType.SINGLE:
                    processed_messages.append(processed_message.payload)
                elif processed_message.type == ResponseType.MULTIPLE:
                    processed_messages.extend(processed_message.payload)

        if self.config.duplicate_catcher:
            # If we are using duplicate catcher, it means that we have a state to save, so we need to store the messages.
            for message in processed_messages:
                pass
                # TODO: See how we should store the messages
                # self.log_guardian.store_new_connection_message(message.payload)

        if processed_messages:
            if self.config.is_topic:
                self.send_messages_topic(
                    processed_messages, messages.client_id, messages.message_id
                )
            else:
                self.send_messages(
                    processed_messages, messages.client_id, messages.message_id
                )
            # Log the messages sent
            self.log_guardian.message_sent()

        if self.config.duplicate_catcher:
            # If we are using duplicate catcher, it means that we have a state to save, so we need to store the messages.
            duplicate_catcher_states = {}
            for client_id, duplicate_catcher in self.duplicate_catchers.items():
                duplicate_catcher_states[client_id] = duplicate_catcher.get_state()
            self.log_guardian.store_duplicate_catchers(duplicate_catcher_states)

    def process_duplicate_catcher(self, messages):
        """
        Returns True if the message is a duplicate.
        """
        duplicate_catcher = self.get_duplicate_catcher(messages.client_id)
        return duplicate_catcher.is_duplicate(messages.message_id)

    def send_messages_topic(self, messages, client_id, message_id):
        # message: (topic, message)
        messages_by_topic = {}
        for message in messages:
            messages_by_topic[message[0]] = messages_by_topic.get(message[0], []) + [
                message[1]
            ]
        for topic, messages in messages_by_topic.items():
            message_to_send = ProtocolMessage(client_id, message_id, messages)
            self.communication_sender.send_all(
                message_to_send,
                routing_key=str(topic),
                output_fields_order=self.config.output_fields,
            )

    def send_messages(self, messages, client_id, message_id):
        message_to_send = ProtocolMessage(client_id, message_id, messages)
        self.communication_sender.send_all(
            message_to_send, output_fields_order=self.config.output_fields
        )

    def handle_eof(self, client_id):
        messages = []
        message = self.get_processor(client_id).finish_processing()
        if message:
            if message.type == ResponseType.SINGLE:
                messages.append(message.payload)
            elif message.type == ResponseType.MULTIPLE:
                messages.extend(message.payload)

        if self.config.is_topic:
            # TODO: If needed, we should move the topic name the finish_processing of the processor.
            DEFAULT_TOPIC_EOF = "1"
            self.communication_sender.send_eof(client_id, routing_key=DEFAULT_TOPIC_EOF)
            return
        if messages:
            # TODO: We send the message with message_id as the replica_id to differentiate the protocol EOF messages sent by other replicas.
            #       Maybe we should change this to other value.
            self.send_messages(messages, client_id, self.config.replica_id)
        if self.config.send_eof:
            self.communication_sender.send_eof(client_id)

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: shutdown | result: in_progress")
        # TODO: neccesary to call finish_processing?
        # self.processor.finish_processing()
        if self.communication_receiver:
            self.communication_receiver.close()
        if self.communication_sender:
            self.communication_sender.close()
        logging.info("action: shutdown | result: success")
