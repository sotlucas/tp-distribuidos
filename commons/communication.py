from enum import Enum
import time
import pika
import logging
from commons.duplicate_catcher import DuplicateCatcher
from commons.flight_parser import FlightParser
from commons.message import (
    Message,
    MessageType,
    EOFMessage,
    EOFDiscoveryMessage,
    EOFAggregationMessage,
    EOFFinishMessage,
)
from pika.exceptions import ConnectionWrongStateError


class ACKType(Enum):
    ACK = 1
    NACK = 2


class CommunicationConnection:
    """
    Class to wrap the connection to the RabbitMQ server

    It is not thread safe, so it should be used only by one thread

    To use it, first call the `connect` method, then the `channel` method to get a channel to the RabbitMQ server
    and finally the close method to `close` the connection sending all messages waiting on the buffer
    """

    def __init__(self, rabbit_host):
        self.rabbit_host = rabbit_host
        # reduce log level for pika
        logging.getLogger("pika").setLevel(logging.WARNING)

    def connect(self):
        """
        Connects to the RabbitMQ server
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.rabbit_host)
        )

    def channel(self):
        """
        Returns a channel to the RabbitMQ server
        """
        return self.connection.channel()

    def close(self):
        """
        Closes the connection sending all messages waiting on the buffer
        This should be called always to ensure all messages have been sent
        """
        try:
            self.connection.close()
        except ConnectionWrongStateError:
            # It means the connection is already closed
            pass


class Communication:
    """
    Abstract class to be used by the Communication classes

    ### Parameters
    - config : CommunicationSenderConfig
        The config for the sender
    - connection : CommunicationConnection
        The connection to the RabbitMQ server
    - log_guardian : LogGuardian
        The log guardian to store the messages received and sent
    """

    def __init__(self, config, connection, log_guardian):
        self.config = config
        self.connection = connection
        self.parser = FlightParser(self.config.delimiter)
        self.log_guardian = log_guardian

        # {client_id: [message_id]}
        self.possible_duplicates = self.log_guardian.get_possible_duplicates()

    def close(self):
        """
        Closes the connection sending all messages waiting on the buffer
        This should be called always to ensure all messages have been sent
        """
        self.connection.close()


class CommunicationReceiverConfig:
    """
    Class to configure the CommunicationReceiver classes

    ### Attributes
    - input : str
        The input where the messages will be received, it can be a queue or an exchange
    - replica_id : int
        The id of the replica for the processor in the distributed system
    - replicas_count : int
        The number of replicas for the processor in the distributed system

    ### Optional attributes
    - routing_key : str
        The routing key to bind the input queue to the input exchange
    - input_diff_name : str
        The input_diff_name used to replicate the input queue, it is used to differentiate the queues for different exchange subscribers
    - delimiter : str
        The delimiter used to parse the messages
    - use_duplicate_catcher : bool
        If True, it uses the duplicate catcher to avoid processing duplicate messages.
        This only should be used if a specific message always goes to the same processor, even if it was requeued. Like, for example, if before we have a LoadBalancer.
    - load_balancer_send_multiply : int
        If this is set, it means that this communication is in a LoadBalancer, this is important because the LoadBalancer sends multiple messages from 1 message received.
        This is used to count the duplicates correctly. The number is the number of messages sent from 1 message, most likely the number of replicas of the next processor.
    """

    def __init__(
        self,
        input,
        replica_id,  # TODO: Now every replica needs to know the replica_id, removed default
        replicas_count,
        routing_key="",
        input_diff_name="",
        delimiter=",",
        use_duplicate_catcher=False,
        load_balancer_send_multiply=None,
    ):
        self.input = input
        self.replica_id = replica_id
        self.replicas_count = replicas_count
        self.routing_key = routing_key
        self.input_diff_name = input_diff_name
        self.delimiter = delimiter
        self.use_duplicate_catcher = use_duplicate_catcher
        self.load_balancer_send_multiply = load_balancer_send_multiply


class CommunicationReceiver(Communication):
    """
    Abstract class to be used by the CommunicationReceiver classes

    To use it, first call the `bind` method to bind the receiver to the input queue or exchange
    and then the `start` method to start receiving messages
    """

    def __init__(self, config, connection, log_guardian):
        super().__init__(config, connection, log_guardian)

        # {client_id: messages_received}
        self.messages_received = self.log_guardian.get_messages_received()

        # {client_id: [DuplicateCatcher]}
        self.duplicate_catchers = {}

        if self.config.use_duplicate_catcher:
            # Restore duplicate catcher state
            for (
                client_id,
                duplicate_catcher_state,
            ) in log_guardian.get_duplicate_catchers().items():
                duplicate_catcher = DuplicateCatcher(duplicate_catcher_state)
                self.duplicate_catchers[client_id] = duplicate_catcher

    def bind(self, input_callback, eof_callback, sender=None, input_fields_order=None):
        """
        Binds the receiver to the input queue or exchange

        ### Parameters
        - input_callback : function
            - Function to be called when a message is received
        - eof_callback : function
            - Function to be called when the EOF is received

        ### Optional parameters
        - sender : CommunicationSender
            - Sender to be used when the EOF is received. It sincronizes the EOF propagation, getting how many messages have been sent.
        - input_fields_order : list of str
            - List of the input fields in the order they will be received, used to parse the messages
        """
        # We connect here because if we connect in the __init__ it it can be closed by the connection for inactivity
        self.connection.connect()
        self.channel = self.connection.channel()
        self.declare_input()

        self.input_callback = input_callback
        self.eof_callback = eof_callback
        self.sender = sender
        self.input_fields_order = input_fields_order

        self.channel.basic_qos(prefetch_count=10)
        self.channel.basic_consume(
            queue=self.input_queue, on_message_callback=self.callback
        )

    def start(self):
        """
        Starts the receiver
        """
        self.channel.start_consuming()

    def stop(self):
        """
        Stops the receiver
        """
        logging.debug("Stopping receiver")
        self.channel.queue_delete(queue=self.input_queue)
        self.channel.stop_consuming()

    def callback(self, ch, method, properties, body):
        """
        Callback to be called when a message is received, it calls the input_callback function with the message as parameter
        """
        try:
            message = Message.from_bytes(body)
        except Exception as e:
            logging.exception(f"Error parsing message: {e}")
            return

        if message.message_type == MessageType.PROTOCOL:
            # First we check if it is a duplicate if we are using the duplicate catcher
            if self.config.use_duplicate_catcher:
                if self.check_duplicate(message):
                    logging.debug(
                        f"Duplicate catcher detected duplicate message {message.message_id}, discarding it"
                    )

                    # It is a duplicate, so we send an NACK with requeue=False to delete the message
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    return

            logging.debug("Received protocol message")
            self.log_guardian.new_message_received(
                message.message_id, message.client_id
            )

            # TODO: Check if this is ok
            # Check redeliver to see if it is a duplicate
            # We need to add redelivered messages to the possible_duplicates, because
            # another replica may have tried to send the ACK but it failed just after sending it,
            # so the ack is lost and the message is redelivered.
            if method.redelivered:
                logging.debug(
                    f"Message {message.message_id} has been redelivered, adding it to the possible_duplicates"
                )
                self.possible_duplicates[
                    message.client_id
                ] = self.possible_duplicates.get(message.client_id, []) + [
                    message.message_id
                ]

            ack_type = self.handle_protocol(message)

            self.log_guardian.store_messages_received(self.messages_received)
            self.log_guardian.store_possible_duplicates(self.possible_duplicates)

            if self.sender:
                self.log_guardian.store_messages_sent(self.sender.messages_sent)

            if self.config.use_duplicate_catcher:
                # We store the duplicate catchers states only if we are using the duplicate catcher
                duplicate_catcher_states = {}
                for client_id, duplicate_catcher in self.duplicate_catchers.items():
                    duplicate_catcher_states[client_id] = duplicate_catcher.get_state()
                self.log_guardian.store_duplicate_catchers(duplicate_catcher_states)

            self.log_guardian.finish_storing_message()

        elif message.message_type == MessageType.EOF:
            logging.debug("Received EOF")
            logging.debug("Received {}".format(body))
            ack_type = self.handle_eof(message)

        elif message.message_type == MessageType.EOF_DISCOVERY:
            logging.debug("Received EOF discovery")
            logging.debug("Received {}".format(body))
            ack_type = self.handle_eof_discovery(message)

        elif message.message_type == MessageType.EOF_AGGREGATION:
            logging.debug("Received EOF aggregation")
            logging.debug("Received {}".format(body))
            ack_type = self.handle_eof_aggregation(message)

        elif message.message_type == MessageType.EOF_FINISH:
            logging.debug("Received EOF finish")
            logging.debug("Received {}".format(body))
            ack_type = self.handle_eof_finish(message)

        if ack_type == ACKType.NACK:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            # Default is ACK
            ch.basic_ack(delivery_tag=method.delivery_tag)

            if message.message_type == MessageType.PROTOCOL:
                # We only commit the message if it is a protocol message, because the EOF messages are requeued
                self.log_guardian.commit_message()

    def check_duplicate(self, message):
        """
        Checks if the message is a duplicate using the duplicate catcher
        """
        if message.client_id not in self.duplicate_catchers:
            self.duplicate_catchers[message.client_id] = DuplicateCatcher()

        duplicate_catcher = self.duplicate_catchers[message.client_id]

        return duplicate_catcher.is_duplicate(message.message_id)

    def handle_protocol(self, message):
        """
        Handles the protocol message, calling the input_callback function
        """
        start_time = time.time()

        message.payload = message.payload.rstrip().split("\n")
        if self.input_fields_order:
            messages_parsed = [
                self.parser.parse(message, self.input_fields_order)
                for message in message.payload
            ]
            message.payload = messages_parsed

        self.messages_received[message.client_id] = (
            self.messages_received.get(message.client_id, 0) + 1
        )

        logging.debug("Received message_id {}".format(message.message_id))

        try:
            self.input_callback(message)
        except Exception as e:
            logging.exception(f"Error processing message in input_callback: {e}")

        logging.debug("Processed in {} seconds".format(time.time() - start_time))
        return ACKType.ACK

    def handle_eof(self, message):
        # We convert the EOF to a EOFDiscoveryMessage to be able to handle it in the same way
        eof_message = EOFDiscoveryMessage(
            message.client_id,
            message.messages_sent,
            message.possible_duplicates,
            0,
            0,
            [],
            [],
        )
        return self.handle_eof_discovery(eof_message)

    def handle_eof_discovery(self, message):
        if self.config.replica_id in message.replica_id_seen:
            # We have already processed this EOF, so we requeue it with a nack
            logging.debug("EOF discovery already processed, requeueing")
            return ACKType.NACK

        # We create a new EOFDiscoveryMessage with the new data
        new_discovery = self.update_discovery_eof(message)

        if len(new_discovery.replica_id_seen) < self.config.replicas_count:
            # The Discovery EOF has not finished propagating, so we requeue it
            logging.debug("EOF discovery not finished propagating, requeueing")
            self.requeue_eof(new_discovery)
            return ACKType.ACK
        else:
            # The Discovery EOF has finished propagating
            logging.debug("EOF discovery finished propagating")

            # We convert the EOFDiscoveryMessage to a EOFAggregationMessage to be able to handle it in the same way
            aggregation_message = EOFAggregationMessage(
                new_discovery.client_id,
                new_discovery.original_messages_sent,
                new_discovery.original_possible_duplicates,
                new_discovery.messages_received,
                new_discovery.messages_sent,
                new_discovery.possible_duplicates,
                [],
                [],
            )
            return self.handle_eof_aggregation(aggregation_message)

    def handle_eof_aggregation(self, message):
        if self.config.replica_id in message.replica_id_seen:
            # We have already processed this EOF, so we requeue it with a nack
            logging.debug("EOF aggregation already processed, requeueing")
            return ACKType.NACK

        # We create a new EOFAggregationMessage with the new data
        new_aggregation = self.update_aggregation_eof(message)

        if len(new_aggregation.replica_id_seen) < self.config.replicas_count:
            # The Discovery EOF has not finished propagating, so we requeue it
            logging.debug("EOF aggregation not finished propagating, requeueing")
            self.requeue_eof(new_aggregation)
            return ACKType.ACK
        else:
            # The Aggregation EOF has finished propagating
            logging.debug("EOF aggregation finished propagating")

            (
                duplicate_messages_received,
                duplicate_messages_sent,
            ) = self.count_duplicates(new_aggregation.possible_duplicates_processed_by)
            real_messages_received = (
                new_aggregation.messages_received - duplicate_messages_received
            )
            real_messages_sent = new_aggregation.messages_sent - duplicate_messages_sent
            logging.debug(f"Duplicate messages received: {duplicate_messages_received}")
            logging.debug(f"Duplicate messages sent: {duplicate_messages_sent}")

            if real_messages_received == new_aggregation.original_messages_sent:
                # It means that all the messages have been received, so we call the eof_callback
                logging.debug("All real messages received, calling eof_callback")
                logging.debug(f"Real messages received: {real_messages_received}")
                logging.debug(f"Real messages sent: {real_messages_sent}")

                # We change the messages_sent of the sender to the new_messages_sent to sincronize the EOF propagation
                # TODO: This breaks encapsulation, see if we can do it in a better way
                if self.sender:
                    logging.debug(
                        "Updating messages_sent to {}".format(real_messages_sent)
                    )
                    self.sender.messages_sent[message.client_id] = real_messages_sent

                # We change the messages_sent of the sender to the new_messages_sent to sincronize the EOF propagation.
                # We also add the possible_duplicates and original_possible_duplicates to the sender to warn for duplicates.
                # TODO: This breaks encapsulation, see if we can do it in a better way
                if self.sender:
                    logging.debug(
                        "Updating possible_duplicates to {}".format(
                            new_aggregation.possible_duplicates
                            + new_aggregation.original_possible_duplicates
                        )
                    )
                    self.sender.possible_duplicates[message.client_id] = (
                        new_aggregation.possible_duplicates
                        + new_aggregation.original_possible_duplicates
                    )

                if not self.config.routing_key:
                    # We call the eof_callback only if we are in a queue, because in a topic exchange we call it in the handle_eof_finish
                    # This is because the Grouper hangs if we call the eof_callback here, withoud requeueing the EOF.
                    # TODO: If we are deleating the data in the handle_eof_finish we might need to change this.
                    self.eof_callback(message.client_id)

                # We create a new EOFFinishMessage and handle it to notify the other replicas that the EOF has finished
                eof_finish = EOFFinishMessage(
                    message.client_id,
                    [],
                )
                return self.handle_eof_finish(eof_finish)
            else:
                # It means there are remaining messages to receive.
                logging.debug("Not all real messages received, requeueing original EOF")
                logging.debug(f"Real messages received: {real_messages_received}")
                logging.debug(f"Real messages sent: {real_messages_sent}")
                logging.debug(
                    f"Original messages sent: {new_aggregation.original_messages_sent}"
                )

                # We requeue the EOF with the original values, to start a new round of EOF propagation

                eof_message = EOFMessage(
                    message.client_id,
                    message.original_messages_sent,
                    message.original_possible_duplicates,
                )
                self.requeue_eof(eof_message)
                return ACKType.ACK

    def count_duplicates(self, possible_duplicates_processed_by):
        """
        Counts the duplicates message received and sent
        """
        duplicates_counter = {}
        for processed_message in possible_duplicates_processed_by:
            duplicates_counter[processed_message] = (
                duplicates_counter.get(processed_message, 0) + 1
            )
        for processed_message, count in duplicates_counter.items():
            duplicates_counter[processed_message] = count - 1
            if count == 0:
                duplicates_counter.pop(processed_message)

        duplicate_messages_received = sum(duplicates_counter.values())
        duplicate_messages_sent = sum(
            [
                count
                for processed_message, count in duplicates_counter.items()
                if processed_message.sent
            ]
        )

        if self.config.load_balancer_send_multiply:
            # We are in a LoadBalancer, so we need to multiply the duplicate_messages_sent
            # to count the duplicates sent correctly and send the correct number of real messages

            # TODO: There is a border case when the LoadBalancer does not send the message to all the next replicas,
            #       this can happen if, for example, the batch of message is smaller than the number of replicas.
            #       Fix this if needed.
            duplicate_messages_sent *= self.config.load_balancer_send_multiply

        return duplicate_messages_received, duplicate_messages_sent

    def handle_eof_finish(self, message):
        if self.config.replica_id in message.replica_id_seen:
            # We have already processed this EOF, so we requeue it with a nack
            logging.debug("EOF aggregation already processed, requeueing")
            return ACKType.NACK

        new_finish = self.update_finish_eof(message)

        if len(new_finish.replica_id_seen) < self.config.replicas_count:
            # The Finish EOF has not finished propagating, so we requeue it
            self.requeue_eof(new_finish)

        if self.config.routing_key:
            # We are in a topic exchange, so we execute the eof_callback
            self.eof_callback(message.client_id)

        # TODO: Here we should delete all the data related to the client_id and then save
        #       Watch out that if there are multiple EOF at the same time, they may never end, see if we can solve it
        #       Maybe if we search for the client_id when we receive a any EOF message and if it is not found we assume it is finished

        return ACKType.ACK

    def update_discovery_eof(self, message):
        """
        Updates the Discovery EOF message with the new data
        """
        new_messages_received = message.messages_received + self.messages_received.get(
            message.client_id, 0
        )
        # TODO: This breaks encapsulation, see if we can do it in a better way
        sender_messages_sent = (
            self.sender.get_client_messages_sent(message.client_id)
            if self.sender
            else 0
        )
        new_messages_sent = message.messages_sent + sender_messages_sent

        new_possible_duplicates = (
            message.possible_duplicates
            + self.possible_duplicates.get(message.client_id, [])
        )

        new_replica_id_seen = message.replica_id_seen + [self.config.replica_id]

        return EOFDiscoveryMessage(
            message.client_id,
            message.original_messages_sent,
            message.original_possible_duplicates,
            new_messages_received,
            new_messages_sent,
            new_possible_duplicates,
            new_replica_id_seen,
        )

    def update_aggregation_eof(self, message):
        """
        Updates the Aggregation EOF message with the new data
        """
        new_replica_id_seen = message.replica_id_seen + [self.config.replica_id]

        if not self.config.use_duplicate_catcher:
            # We search if we processed any of the possible duplicates and original possible duplicates
            # to add them to the possible_duplicates_processed_by
            duplicates_processed = self.log_guardian.search_for_duplicate_messages(
                message.client_id,
                message.possible_duplicates + message.original_possible_duplicates,
            )
            new_possible_duplicates_processed_by = (
                message.possible_duplicates_processed_by + duplicates_processed
            )
        else:
            # We don't need to search for duplicates, because we are using the duplicate catcher
            new_possible_duplicates_processed_by = (
                message.possible_duplicates_processed_by
            )

        return EOFAggregationMessage(
            message.client_id,
            message.original_messages_sent,
            message.original_possible_duplicates,
            message.messages_received,
            message.messages_sent,
            message.possible_duplicates,
            new_replica_id_seen,
            new_possible_duplicates_processed_by,
        )

    def update_finish_eof(self, message):
        """
        Updates the Finish EOF message with the new data
        """
        new_replica_id_seen = message.replica_id_seen + [self.config.replica_id]

        return EOFFinishMessage(
            message.client_id,
            new_replica_id_seen,
        )

    def requeue_eof(self, message):
        """
        Requeues the EOF

        The EOF is requeued to the input queue, so it is sent to the other instances.
        """
        logging.debug(f"Requeueing EOF in {self.input_queue}")

        message_bytes = message.to_bytes()
        exchange, routing_key = self.get_exchange_and_routing_key()

        self.send_requeue(message_bytes, exchange, routing_key)

    def get_exchange_and_routing_key(self):
        exchange = ""
        routing_key = self.input_queue

        if self.config.routing_key:
            # We are in a topic exchange, so we only requeue the EOF to the next replica
            exchange = self.config.input
            # This is calculated as this because the replicas are numbered from 1 to replicas_count
            next_replica = str(
                (self.config.replica_id % self.config.replicas_count) + 1
            )
            routing_key = next_replica

        return exchange, routing_key

    def send_requeue(self, message, exchange, routing_key):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )


class CommunicationReceiverExchange(CommunicationReceiver):
    def declare_input(self):
        """
        Declares the input exchange and binds the input queue to it.

        If the routing_key is empty, it is a fanout exchange, so the messages are sent to all the subscribers.
        """
        # To differentiate the queues for different exchange subscribers, we append the input_diff_name to the input queue name.
        # And we also append the routing key to the input queue name for the same reason.
        # This is because the input queue name is used as the exchange name.
        # So we do this to replicate the filters and function as workers.
        # TODO: See if adding an environment variable to solve this in a better way.
        input_queue_name = (
            self.config.input + self.config.input_diff_name + self.config.routing_key
        )
        input_queue = self.channel.queue_declare(queue=input_queue_name, durable=True)
        self.input_queue = input_queue.method.queue

        exchange_type = "fanout" if self.config.routing_key == "" else "topic"
        self.channel.exchange_declare(
            exchange=self.config.input, exchange_type=exchange_type, durable=True
        )

        self.channel.queue_bind(
            exchange=self.config.input,
            queue=self.input_queue,
            routing_key=self.config.routing_key,
        )

        # We need to confirm the delivery to ensure the messages are sent
        self.channel.confirm_delivery()


class CommunicationReceiverQueue(CommunicationReceiver):
    def declare_input(self):
        self.input_queue = self.config.input
        self.channel.queue_declare(queue=self.input_queue, durable=True)


# ===================================================================================================================================================================================


class CommunicationSenderConfig:
    """
    Class to configure the CommunicationSender classes

    ### Attributes
    - output : str
        The output where the messages will be sent, it can be a queue or an exchange

    ### Optional attributes
    - delimiter : str
        The delimiter used to parse the messages
    """

    def __init__(self, output, delimiter=","):
        self.output = output
        self.delimiter = delimiter


class CommunicationSender(Communication):
    """
    Abstract class to be used by the CommunicationSender classes
    """

    def __init__(self, config, connection, log_guardian):
        super().__init__(config, connection, log_guardian)
        self.active = False

        # {client_id: messages_sent}
        self.messages_sent = self.log_guardian.get_messages_sent()

    def activate(self):
        if not self.active:
            # We connect here because if we connect in the __init__ it it can be closed by the connection for inactivity
            self.connection.connect()
            self.channel = self.connection.channel()
            self.declare_output()
            self.active = True

    def send_all(self, messages, routing_key="", output_fields_order=None):
        """
        Sends a batch of messages to the output
        """
        if output_fields_order:
            messages_serialized = [
                self.parser.serialize(message, output_fields_order)
                for message in messages.payload
            ]
            messages.payload = messages_serialized
        messages.payload = "\n".join(messages.payload)

        self.send(messages, routing_key)
        # Log the messages sent
        self.log_guardian.message_sent()

    def send(self, message, routing_key=""):
        """
        Sends a message to the output
        """
        raise NotImplementedError("The send method must be implemented by the subclass")

    def send_to(self, message, exchange, routing_key):
        self.activate()
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message.to_bytes(),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        self.messages_sent[message.client_id] = (
            self.messages_sent.get(message.client_id, 0) + 1
        )

    def send_eof(self, client_id, routing_key=""):
        """
        Function to send the EOF to propagate through the distributed system.

        Protocol:
        - First byte is 0
        - Next 8 bytes are the number of messages sent

        | EOF | messages_sent |
        0     1               9
        """
        logging.debug("Sending EOF")
        messages_sent = self.get_client_messages_sent(client_id)
        logging.debug("Messages sent: {}".format(messages_sent))
        message = EOFMessage(
            client_id, messages_sent, self.possible_duplicates.get(client_id, [])
        )
        self.send(message, routing_key)

    def get_client_messages_sent(self, client_id):
        return self.messages_sent.get(client_id, 0)

    def stop(self):
        """
        Stops the sender
        """
        self.close()


class CommunicationSenderExchange(CommunicationSender):
    """
    Class to send messages to an exchange
    """

    def declare_output(self):
        # TODO: check if we need to declare the exchange
        pass

    def send(self, message, routing_key=""):
        """
        Sends a message to the output exchange with the routing_key.
        If no routing_key is specified, it is sent to all the subscribers of the exchange.
        """
        self.send_to(message, self.config.output, routing_key)


class CommunicationSenderQueue(CommunicationSender):
    """
    Class to send messages to a queue
    """

    def declare_output(self):
        self.channel.queue_declare(queue=self.config.output, durable=True)

    def send(self, message, routing_key=""):
        """
        Sends a message to the output queue.
        """
        self.send_to(message, "", self.config.output)
