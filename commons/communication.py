import pika
import logging


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
    """

    def __init__(self, input, replicas_count, routing_key="", input_diff_name=""):
        self.input = input
        self.replicas_count = replicas_count
        self.routing_key = routing_key
        self.output = input_diff_name


class CommunicationReceiver(Communication):
    """
    Abstract class to be used by the CommunicationReceiver classes

    To use it, first call the `bind` method to bind the receiver to the input queue or exchange
    and then the `start` method to start receiving messages
    """

    def __init__(self, config, connection):
        super().__init__(config, connection)
        # A receiver is not active if it has received an EOF, until it it receives a message again.
        # This is to avoid receiving the EOF multiple times.
        self.active = True

    def bind(self, input_callback, eof_callback):
        """
        Binds the receiver to the input queue or exchange

        ### Parameters
        - input_callback : function
            Function to be called when a message is received
        - eof_callback : function
            Function to be called when the EOF is received
        """
        # We connect here because if we connect in the __init__ it it can be closed by the connection for inactivity
        self.connection.connect()
        self.channel = self.connection.channel()
        self.declare_input()

        self.input_callback = input_callback
        self.eof_callback = eof_callback

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
        logging.debug("Received {}".format(body))
        message = self.intercept(body)
        # TODO: revisar
        if not message:
            return
        # TODO: Crear un parser para los mensajes
        message = message.decode("utf-8")
        self.input_callback(message)

    def intercept(self, message):
        """
        Detects if the message is an EOF message and if it is requeues it for the other instances to process it.
        """
        if not message[0] == 0:
            self.active = True
            return message
        # Message is an EOF
        if not self.active:
            # It means the EOF has already been received, so we requeue it
            self.requeue(message)
            return
        # It means it is the first EOF, so we deactivate the receiver and call the callback
        self.active = False

        if len(message) == 1:
            # It means it is the first EOF, so we add the TTL to the message to finish all the workers before continuing
            ttl = self.config.replicas_count - 1
        else:
            # It means it is a requeued EOF, so we get the TTL from the message
            ttl = int.from_bytes(message[1:5], "big")

        if ttl > 0:
            # Decrease the TTL and requeue
            ttl_nuevo = ttl - 1
            message = b"\0" + ttl_nuevo.to_bytes(4, "big")
            self.requeue(message)
        else:
            # The EOF has finished propagating, so we call the callback
            self.eof_callback()

    def requeue(self, message):
        """
        Requeues the EOF decreasing its TTL by 1.

        The EOF is requeued to the input queue, so it is sent to the other instances.
        """
        self.channel.basic_publish(
            exchange="",
            routing_key=self.input_queue,
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
        # To differentiate the queues for different exchange subscribers, we append the output queue name to the input queue name.
        # And we also append the routing key & input queue sufix to the input queue name. For the same reason.
        # This is because the input queue name is used as the exchange name.
        # So we do this to replicate the filters and function as workers.
        # TODO: See if adding an environment variable to solve this in a better way.
        input_queue_name = (
            self.config.input + self.config.output + self.config.routing_key
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
        # Bind to EOF queue TODO: explicar
        self.channel.queue_bind(
            exchange=self.config.input,
            queue=self.input_queue,
            routing_key="EOF",
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
    """

    def __init__(self, output):
        self.output = output


class CommunicationSender(Communication):
    """
    Abstract class to be used by the CommunicationSender classes
    """

    def __init__(self, config, connection):
        super().__init__(config, connection)
        self.active = False

    def activate(self):
        if not self.active:
            # We connect here because if we connect in the __init__ it it can be closed by the connection for inactivity
            self.connection.connect()
            self.channel = self.connection.channel()
            self.declare_output()
            self.active = True


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
        self.activate()
        self.channel.basic_publish(
            exchange=self.config.output,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def send_eof(self):
        """
        Function to send the EOF to propagate through the distributed system.
        """
        message = b"\0"
        self.send(message, "EOF")


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
        self.activate()
        self.channel.basic_publish(
            exchange="",
            routing_key=self.config.output,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def send_eof(self):
        """
        Function to send the EOF to propagate through the distributed system.
        """
        message = b"\0"
        self.send(message)
