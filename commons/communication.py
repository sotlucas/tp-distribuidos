import time
import pika
import logging
from commons.message_parser import MessageParser


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
        self.connection.close()


class Communication:
    """
    Abstract class to be used by the Communication classes

    ### Parameters
    - config : CommunicationSenderConfig
        The config for the sender
    - connection : CommunicationConnection
        The connection to the RabbitMQ server
    """

    def __init__(self, config, connection):
        self.config = config
        self.connection = connection
        self.parser = MessageParser(self.config.delimiter)

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
    - replicas_count : int
        The number of replicas of the receiver that will be running

    ### Optional attributes
    - routing_key : str
        The routing key to bind the input queue to the input exchange
    - input_diff_name : str
        The input_diff_name used to replicate the input queue, it is used to differentiate the queues for different exchange subscribers
    - replica_id : int
        The replica_id used to replicate the eof only in topic exchanges
    - delimiter : str
        The delimiter used to parse the messages
    """

    def __init__(
        self,
        input,
        replicas_count,
        routing_key="",
        input_diff_name="",
        replica_id=1,
        delimiter=",",
    ):
        self.input = input
        self.replicas_count = replicas_count
        self.routing_key = routing_key
        self.input_diff_name = input_diff_name
        self.replica_id = replica_id
        self.delimiter = delimiter


class CommunicationReceiver(Communication):
    """
    Abstract class to be used by the CommunicationReceiver classes

    To use it, first call the `bind` method to bind the receiver to the input queue or exchange
    and then the `start` method to start receiving messages
    """

    def __init__(self, config, connection):
        super().__init__(config, connection)
        self.messages_received = 0

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

        self.channel.basic_qos(prefetch_count=100)
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
        self.channel.stop_consuming()
        self.close()

    def callback(self, ch, method, properties, body):
        """
        Callback to be called when a message is received, it calls the input_callback function with the message as parameter
        """
        ch.basic_ack(delivery_tag=method.delivery_tag)
        start_time = time.time()
        logging.debug("Received {}".format(body))
        message = self.intercept(body)
        # TODO: revisar
        if not message:
            return
        self.messages_received += 1
        message = message.decode("utf-8").rstrip()
        messages = message.split("\n")
        if self.input_fields_order:
            messages_parsed = [
                self.parser.parse(message, self.input_fields_order)
                for message in messages
            ]
            messages = messages_parsed
        print(messages)
        try:
            self.input_callback(messages)
            logging.debug("Processed in {} seconds".format(time.time() - start_time))
        except Exception as e:
            logging.info(f"Message: {message}")
            logging.exception(f"Error processing message: {e}")

    def intercept(self, message):
        """
        Detects if the message is an EOF message, if not, it returns the message.
        """
        EOF = 0
        if not message[0] == EOF:
            return message
        self.handle_eof(message)

    def handle_eof(self, message):
        """
        Handles the EOF message.

        Protocol first EOF:
        - First byte is 0
        - Next 8 bytes are the number of messages sent

        0     1               9
        | EOF | messages_sent |


        Protocol requeued EOF:
        - First byte is 0
        - Next 4 bytes are the TTL
        - Next 8 bytes are the number of remaining messages to receive
        - Next 8 bytes are the number of messages sent
        - Next 8 bytes are the number of messages sent by the sender

        0     1     5                   13              21                     29
        | EOF | TTL | remaining_messages | messages_sent | messages_sent_sender |


        Special topic exchange EOF:
        - First byte is 0
        - Next 8 bytes is the string "callback"
        """
        special_topic_exchange_eof = b"\0callback"
        if message == special_topic_exchange_eof:
            logging.debug("Received special EOF for topic exchanges")
            # It means it is a special EOF for topic exchanges
            self.eof_callback()
            return

        (
            ttl,
            remaining_messages,
            messages_sent,
            new_messages_sent,
            new_remaining_messages,
            sender_messages_sent,
        ) = self.get_new_eof_parameters(message)

        if ttl > 0:
            new_ttl = ttl - 1
            if self.config.routing_key == "":
                # The EOF has not finished propagating, so we requeue it
                self.requeue(
                    new_ttl,
                    new_remaining_messages,
                    new_messages_sent,
                    sender_messages_sent,
                )

                # We stop the receiver to avoid receiving more messages
                # TODO: If we need to receive more messages in the future, we need to change this
                #       Like creating a new receiver from outside or something like that
                self.stop()
            else:
                # We are in a topic exchange, so we only requeue the EOF to the next replica
                self.requeue_topic(
                    new_ttl,
                    new_remaining_messages,
                    new_messages_sent,
                    sender_messages_sent,
                )

        else:
            # The EOF has finished propagating
            if new_remaining_messages == 0:
                if self.config.routing_key == "":
                    # It means all the messages have been received, so we call the eof_callback

                    # We change the messages_sent of the sender to the new_messages_sent to sincronize the EOF propagation
                    # TODO: This breaks encapsulation, see if we can do it in a better way
                    if self.sender:
                        logging.debug(
                            "Updating messages_sent to {}".format(new_messages_sent)
                        )
                        self.sender.messages_sent = new_messages_sent
                    self.eof_callback()

                else:
                    # We are in a topic exchange, so we only requeue the EOF to the next replica
                    self.requeue_special_topic()

            else:
                if self.config.routing_key == "":
                    # It means there are remaining messages to receive, so we wait for them requeuing the EOF
                    # We requeue with the same received values, because we will receive again the EOF and we will update them
                    self.requeue(
                        ttl, remaining_messages, messages_sent, sender_messages_sent
                    )
                else:
                    # We are in a topic exchange, so we requeue the original EOF to the next replica
                    self.requeue_original_eof_topic(sender_messages_sent)

    def get_new_eof_parameters(self, message):
        FIRST_EOF_LENGTH = 9

        if len(message) == FIRST_EOF_LENGTH:
            # It means it is the first EOF, so we add the TTL to the message to finish all the workers before continuing
            ttl = self.config.replicas_count - 1
            remaining_messages = int.from_bytes(message[1:9], "big")
            messages_sent = 0
            original_sender_messages_sent = remaining_messages
        else:
            # It means it is a requeued EOF, so we get the TTL and remaining_messages from the message
            ttl = int.from_bytes(message[1:5], "big")
            remaining_messages = int.from_bytes(message[5:13], "big")
            messages_sent = int.from_bytes(message[13:21], "big")
            original_sender_messages_sent = int.from_bytes(message[21:29], "big")

        # TODO: This breaks encapsulation, see if we can do it in a better way
        sender_messages_sent = self.sender.messages_sent if self.sender else 0

        new_messages_sent = sender_messages_sent + messages_sent
        new_remaining_messages = remaining_messages - self.messages_received
        return (
            ttl,
            remaining_messages,
            messages_sent,
            new_messages_sent,
            new_remaining_messages,
            original_sender_messages_sent,
        )

    def requeue(self, ttl, remaining_messages, messages_sent, sender_messages_sent):
        """
        Requeues the EOF decreasing its TTL by 1.

        The EOF is requeued to the input queue, so it is sent to the other instances.
        """
        logging.debug(f"Requeueing EOF in {self.input_queue}")

        message = (
            b"\0"
            + ttl.to_bytes(4, "big")
            + remaining_messages.to_bytes(8, "big")
            + messages_sent.to_bytes(8, "big")
            + sender_messages_sent.to_bytes(8, "big")
        )
        self.channel.basic_publish(
            exchange="",
            routing_key=self.input_queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def requeue_topic(
        self, ttl, remaining_messages, messages_sent, sender_messages_sent
    ):
        """
        Requeues the EOF decreasing its TTL by 1.

        The EOF is requeued to the input queue, so it is sent to the other instances.
        """
        logging.debug(f"Requeueing topic EOF in {self.input_queue}")

        message = (
            b"\0"
            + ttl.to_bytes(4, "big")
            + remaining_messages.to_bytes(8, "big")
            + messages_sent.to_bytes(8, "big")
            + sender_messages_sent.to_bytes(8, "big")
        )
        next_replica = str((self.config.replica_id % self.config.replicas_count) + 1)
        self.channel.basic_publish(
            exchange=self.config.input,
            routing_key=next_replica,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def requeue_original_eof_topic(self, sender_messages_sent):
        """
        Requeues the original EOF to the next replica.
        """
        logging.debug(f"Requeueing original EOF in {self.input_queue}")

        message = b"\0" + sender_messages_sent.to_bytes(8, "big")
        next_replica = str((self.config.replica_id % self.config.replicas_count) + 1)
        self.channel.basic_publish(
            exchange=self.config.input,
            routing_key=next_replica,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def requeue_special_topic(self):
        """
        Requeues the special EOF decreasing its TTL by 1.

        The EOF is requeued to the input queue, so it is sent to the other instances.
        """
        logging.debug(f"Requeueing special EOF in {self.input_queue}")

        message = b"\0callback"
        for i in range(self.config.replicas_count):
            self.channel.basic_publish(
                exchange=self.config.input,
                routing_key=str(i + 1),
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Transient,
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
        input_queue = self.channel.queue_declare(queue=input_queue_name)
        self.input_queue = input_queue.method.queue

        exchange_type = "fanout" if self.config.routing_key == "" else "topic"
        self.channel.exchange_declare(
            exchange=self.config.input, exchange_type=exchange_type
        )

        self.channel.queue_bind(
            exchange=self.config.input,
            queue=self.input_queue,
            routing_key=self.config.routing_key,
        )


class CommunicationReceiverQueue(CommunicationReceiver):
    def declare_input(self):
        self.input_queue = self.config.input
        self.channel.queue_declare(queue=self.input_queue)


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

    def __init__(self, config, connection):
        super().__init__(config, connection)
        self.active = False
        # TODO: check when we need to reset the messages_sent.
        #       Ideally we should reset it when we receive the EOF, start a new batch of messages.
        #       Same for the receiver.
        self.messages_sent = 0

    def activate(self):
        if not self.active:
            # We connect here because if we connect in the __init__ it it can be closed by the connection for inactivity
            self.connection.connect()
            self.channel = self.connection.channel()
            self.declare_output()
            self.active = True

    def send_to(self, message, exchange, routing_key):
        self.activate()
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )
        self.messages_sent += 1


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

    def send_all(self, messages, routing_key="", output_fields_order=None):
        """
        Sends a batch of messages to the output
        """
        if output_fields_order:
            messages = [
                self.parser.serialize(message, output_fields_order)
                for message in messages
            ]
        if len(messages) > 0:
            self.send("\n".join(messages), routing_key)

    def send_eof(self, routing_key=""):
        """
        Function to send the EOF to propagate through the distributed system.

        Protocol:
        - First byte is 0
        - Next 8 bytes are the number of messages sent

        | EOF | messages_sent |
        0     1               9
        """
        logging.debug("Sending EOF")
        message = b"\0" + self.messages_sent.to_bytes(8, "big")
        self.send(message, routing_key)


class CommunicationSenderQueue(CommunicationSender):
    """
    Class to send messages to a queue
    """

    def declare_output(self):
        self.channel.queue_declare(queue=self.config.output)

    def send(self, message):
        """
        Sends a message to the output queue.
        """
        self.send_to(message, "", self.config.output)

    def send_all(self, messages, output_fields_order=None):
        """
        Sends a batch of messages to the output
        """
        if output_fields_order:
            messages = [
                self.parser.serialize(message, output_fields_order)
                for message in messages
            ]
        if len(messages) > 0:
            self.send("\n".join(messages))

    def send_eof(self):
        """
        Function to send the EOF to propagate through the distributed system.

        Protocol:
        - First byte is 0
        - Next 8 bytes are the number of messages sent

        | EOF | messages_sent |
        0     1               9
        """
        logging.debug("Sending EOF")
        message = b"\0" + self.messages_sent.to_bytes(8, "big")
        self.send(message)
