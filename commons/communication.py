import pika
import logging


class Communication:
    """
    Abstract class to be used by the CommunicationSender and CommunicationReceiver classes
    """

    def __init__(self):
        # reduce log level for pika
        logging.getLogger("pika").setLevel(logging.WARNING)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.config.rabbit_host)
        )
        self.channel = self.connection.channel()

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
    - rabbit_host : str
        The rabbit host to connect to
    - input : str
        The input where the messages will be received, it can be a queue or an exchange
    - replicas_count : int
        The number of replicas of the receiver that will be running
    - input_callback : function
        The function that will be called when a message is received
    - output_callback : function
        The function that will be called when a message is sent
    - eof_callback : function
        The function that will be called when the EOF is received
    """

    def __init__(
        self,
        rabbit_host,
        input,
        replicas_count,
        input_callback,
        output_callback,
        eof_callback,
    ):
        self.rabbit_host = rabbit_host
        self.input = input
        self.replicas_count = replicas_count
        self.input_callback = input_callback
        self.output_callback = output_callback
        self.eof_callback = eof_callback


class CommunicationReceiver(Communication):
    """
    Abstract class to be used by the CommunicationReceiver classes
    """

    def __init__(self):
        super().__init__()
        self.channel.basic_consume(
            queue=self.input_queue, on_message_callback=self.callback
        )
        self.channel.basic_qos(prefetch_count=1)

    def run(self):
        """
        Starts the receiver
        """
        self.channel.start_consuming()

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
        self.config.input_callback(message)

    def intercept(self, message):
        """
        Detects if the message is an EOF message and if it is requeues it for the other instances to process it.
        """
        if not message[0] == 0:
            return message
        # Message is an EOF

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
            self.config.eof_callback()

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
    def __init__(self, config, routing_key=""):
        self.config = config
        self.declare_input(routing_key)
        super().__init__()

    def declare_input(self, routing_key):
        """
        Declares the input exchange and binds the input queue to it.

        If the routing_key is empty, it is a fanout exchange, so the messages are sent to all the subscribers.
        """
        # Create a random queue to differentiate the queues for different exchange subscribers
        input_queue = self.channel.queue_declare(queue="")
        self.input_queue = input_queue.method.queue

        exchange_type = "fanout" if routing_key == "" else "topic"
        self.channel.exchange_declare(
            exchange=self.config.input, exchange_type=exchange_type
        )

        self.channel.queue_bind(
            exchange=self.config.input,
            queue=self.input_queue,
            routing_key=routing_key,
        )
        # Bind to EOF queue TODO: explicar
        self.channel.queue_bind(
            exchange=self.config.input,
            queue=self.input_queue,
            routing_key="EOF",
        )


class CommunicationReceiverQueue(CommunicationReceiver):
    def __init__(self, config):
        self.config = config
        self.declare_input()
        super().__init__()

    def declare_input(self):
        self.input_queue = self.config.input
        self.channel.queue_declare(queue=self.input_queue)


# ===================================================================================================================================================================================


class CommunicationSenderConfig:
    """
    Class to configure the CommunicationSender classes

    ### Attributes
    - rabbit_host : str
        The rabbit host to connect to
    - output : str
        The output where the messages will be sent, it can be a queue or an exchange
    """

    def __init__(self, rabbit_host, output):
        self.rabbit_host = rabbit_host
        self.output = output


class CommunicationSender(Communication):
    """
    Abstract class to be used by the CommunicationSender classes
    """

    def __init__(self):
        super().__init__()


class CommunicationSenderExchange(CommunicationSender):
    """
    Class to send messages to an exchange
    """

    def __init__(self, config):
        self.config = config
        self.declare_output()
        super().__init__()

    def declare_output(self):
        # TODO: check if we need to declare the exchange
        pass

    def send(self, message, routing_key=""):
        """
        Sends a message to the output exchange with the routing_key.
        If no routing_key is specified, it is sent to all the subscribers of the exchange.
        """
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

    def __init__(self, config):
        self.config = config
        self.declare_output()
        super().__init__()

    def declare_output(self):
        self.channel.queue_declare(queue=self.config.output)

    def send(self, message):
        """
        Sends a message to the output queue.
        """
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
