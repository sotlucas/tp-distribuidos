import pika
import logging


class CommunicationConfig:
    def __init__(self, input_queue, output_queue, rabbit_host, input_type, output_type):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.rabbit_host = rabbit_host
        self.input_type = input_type  # PUBSUB or QUEUE
        self.output_type = output_type  # PUBSUB or QUEUE


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
            self.channel.queue_declare(queue=self.config.output_queue, durable=True)

    def run(self, input_callback):
        self.input_callback = input_callback
        if self.config.input_type == "PUBSUB":
            self.run_exchange()
        else:
            self.run_queue()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.start_consuming()

    def run_queue(self):
        self.channel.queue_declare(queue=self.config.input_queue, durable=True)
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
        result = self.channel.queue_declare(
            queue=self.config.input_queue + self.config.output_queue, durable=True
        )
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.config.input_queue, queue=queue_name)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

    def callback(self, ch, method, properties, body):
        logging.debug("Received {}".format(body))
        output_message = self.input_callback(body.decode("utf-8"))
        if output_message:
            self.send_output(output_message)
            logging.debug("Sent message")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_output(self, message):
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
                delivery_mode=2,  # make message persistent
            ),
        )

    def send_exchange(self, message):
        self.channel.basic_publish(
            exchange=self.config.output_queue,
            routing_key="",
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )
