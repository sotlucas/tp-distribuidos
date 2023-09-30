import pika
import logging


class CommunicationConfig:
    def __init__(self, input_queue, output_queue, rabbit_host):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.rabbit_host = rabbit_host


class Communication:
    def __init__(self, config, input_callback):
        self.config = config
        self.input_callback = input_callback
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.config.rabbit_host)
        )
        self.channel = self.connection.channel()

    def run(self):
        self.channel.queue_declare(queue=self.config.input_queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.config.input_queue, on_message_callback=self.callback
        )
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        logging.info("Received {}".format(body))
        print("Received {}".format(body))
        output_message = self.input_callback(body.decode("utf-8"))
        self.send_output(output_message)
        print("Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_output(self, message):
        self.channel.queue_declare(queue=self.config.output_queue, durable=True)
        self.channel.basic_publish(
            exchange="",
            routing_key=self.config.output_queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )
