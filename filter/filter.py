import pika
import logging
import csv


class FilterConfig:
    def __init__(
        self, input_fields, output_fields, input_queue, output_queue, rabbit_host
    ) -> None:
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.rabbit_host = rabbit_host


class Filter:
    def __init__(self, config):
        self.config = config
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
        # Filtrar columnas
        filtered_row = self.filter(body.decode("utf-8"))
        # Enviar a la cola de output
        self.send_output(filtered_row)
        print("Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def filter(self, message):
        input_fields = self.config.input_fields.split(",")
        reader = csv.DictReader([message], fieldnames=input_fields)
        for row in reader:
            print(row)
            output_fields = self.config.output_fields.split(",")
            filtered_row = [row[key] for key in output_fields]
            return ",".join(filtered_row)

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
