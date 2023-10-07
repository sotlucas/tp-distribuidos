import pika
import logging


class CommunicationConfig:
    def __init__(
        self,
        input_queue,
        output_queue,
        rabbit_host,
        input_type,
        output_type,
        replicas_count,
    ):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.rabbit_host = rabbit_host
        self.input_type = input_type  # PUBSUB or QUEUE
        self.output_type = output_type  # PUBSUB or QUEUE
        self.replicas_count = replicas_count


class Communication:
    def __init__(self, config):
        # reduce log level for pika
        logging.getLogger("pika").setLevel(logging.WARNING)
        self.config = config
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.config.rabbit_host)
        )
        self.channel = self.connection.channel()

        # Declare the output queue
        if self.config.output_type == "QUEUE":
            self.channel.queue_declare(queue=self.config.output_queue)

    def run(self, input_callback=None, output_callback=None, eof_callback=None):
        self.input_callback = input_callback
        # If no output callback is provided, we use the default one, which sends the message to the output rabbit queue.
        self.output_callback = output_callback if output_callback else self.send_output
        # If no eof callback is provided, we use the default one, which sends the eof to the output.
        self.eof_callback = eof_callback if eof_callback else self.send_eof
        if self.config.input_type == "PUBSUB":
            self.run_exchange()
        else:
            self.run_queue()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.start_consuming()

    def run_queue(self):
        self.channel.queue_declare(queue=self.config.input_queue)
        self.channel.basic_consume(
            queue=self.config.input_queue, on_message_callback=self.callback
        )

    def run_exchange(self):
        self.channel.exchange_declare(
            exchange=self.config.input_queue, exchange_type="fanout"
        )
        # To differentiate the queues for different filters, we append the output queue name to the input queue name.
        # This is because the input queue name is used as the exchange name.
        # So we do this to replicate the filters and function as workers.
        input_queue_name = self.config.input_queue + self.config.output_queue
        self.channel.queue_declare(queue=input_queue_name)
        self.channel.queue_bind(
            exchange=self.config.input_queue, queue=input_queue_name
        )
        self.channel.basic_consume(
            queue=input_queue_name, on_message_callback=self.callback
        )

    def callback(self, ch, method, properties, body):
        logging.debug("Received {}".format(body))
        message = self.intercept(body)
        # TODO: revisar
        if not message:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        message = message.decode("utf-8")
        output_message = (
            self.input_callback(message) if self.input_callback else message
        )
        if output_message:
            self.output_callback(output_message)
            logging.debug("Sent message")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_output(self, message):
        """
        Sends the messages to the output depending on its type PUBSUB or QUEUE
        """
        if self.config.output_type == "PUBSUB":
            self.send_exchange(message)
        else:
            self.send_queue(message)

    def send_queue(self, message):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.config.output_queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def send_exchange(self, message):
        self.channel.basic_publish(
            exchange=self.config.output_queue,
            routing_key="",
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Transient,
            ),
        )

    def close(self):
        """
        Closes the connection sending all messages waiting on the buffer

        This should be called always to ensure all messages have been sent
        """
        self.connection.close()

    def intercept(self, message):
        """
        Detects if the message is an EOF message and if it is requeues it for the other instances to process it.
        """
        if message[0] == 0:
            if len(message) == 1:
                # It means is the real EOF, so we add the TTL to the message to finish all the workers before continuing
                ttl = self.config.replicas_count - 1
            else:
                ttl = int.from_bytes(message[1:5], "big")

            if ttl > 0:
                # Decrease the TTL and requeue
                ttl_nuevo = ttl - 1
                self.requeue(ttl_nuevo)
            else:
                # The EOF has finished propagating, so we call the callback
                self.eof_callback()
        else:
            return message

    def requeue(self, ttl):
        """
        Requeues the EOF decreasing its TTL by 1.
        """
        message = b"\0" + ttl.to_bytes(4, "big")

        input_queue_name = (
            self.config.input_queue
            if self.config.input_type == "QUEUE"
            else self.config.input_queue + self.config.output_queue
        )
        self.channel.basic_publish(
            exchange="",
            routing_key=input_queue_name,
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
        self.send_output(message)
