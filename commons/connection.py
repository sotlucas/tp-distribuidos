import logging
import signal
from commons.message import ProtocolMessage
from commons.processor import ResponseType


class ConnectionConfig:
    def __init__(
        self,
        replica_id,
        input_fields=None,
        output_fields=None,
        send_eof=True,
        is_topic=False,
        has_statefull_processor=False,
    ):
        self.replica_id = replica_id
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.send_eof = send_eof
        self.is_topic = is_topic
        self.has_statefull_processor = has_statefull_processor


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

        if self.config.has_statefull_processor:
            # If we have a statefull processor, we need to restore all the processors.
            clients_ids = self.log_guardian.obtain_all_active_clients()
            for client_id in clients_ids:
                processor = self.get_processor(client_id)
                self.restore_statefull_processor(client_id, processor)

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

    def restore_statefull_processor(self, client_id, processor):
        all_messages = self.log_guardian.search_for_all_connection_messages(client_id)

        for message_batch in all_messages:
            for message in message_batch:
                # A statefull processor should not return a response, so we don't need to do anything with it.
                processor.process(message)

    def process(self, messages):
        if self.config.has_statefull_processor:
            # If we have a statefull processor, we also need to save the messages to disk to be able to recover them
            self.save_messages(messages)

        processor = self.get_processor(messages.client_id)
        processed_messages = []
        for message in messages.payload:
            processed_message = processor.process(message)
            if processed_message:
                if processed_message.type == ResponseType.SINGLE:
                    processed_messages.append(processed_message.payload)
                elif processed_message.type == ResponseType.MULTIPLE:
                    processed_messages.extend(processed_message.payload)
                elif processed_message.type == ResponseType.NOT_READY:
                    # If the message is not ready, it means that we need to wait for more messages.
                    # So we need to requeue the message, sending a nack.
                    return True

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
        return False

    def save_messages(self, messages):
        self.log_guardian.store_new_connection_message(messages.payload)

    def send_messages_topic(self, messages, client_id, message_id):
        # message: (topic, message)
        messages_by_topic = {}
        for message in messages:
            messages_by_topic[message[0]] = messages_by_topic.get(message[0], []) + [
                message[1]
            ]
        for topic, messages in messages_by_topic.items():
            logging.debug(
                f"Sending messages to topic {topic} with client_id {client_id}"
            )
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
            # TODO: if self.config.send_eof: ?
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
