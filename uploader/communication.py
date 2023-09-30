import pika
import logging

BUFFER_SIZE = 8192  # 8 KiB
END_OF_MESSAGE = b"\r\n\r\n"


class CommunicationConfig:
    def __init__(self, output_queue, rabbit_host):
        self.output_queue = output_queue
        self.rabbit_host = rabbit_host


class Communication:
    def __init__(self, config):
        # reduce log level for pika
        logging.getLogger("pika").setLevel(logging.WARNING)
        self.config = config
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.config.rabbit_host)
        )
        self.channel = self.connection.channel()

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


class CommunicationBuffer:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def get_line(self):
        while END_OF_MESSAGE not in self.buffer:
            data = self.sock.recv(BUFFER_SIZE)
            if not data:  # socket is closed
                raise ClientDisconnected
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(END_OF_MESSAGE)
        return line.decode()


class ClientDisconnected(Exception):
    pass
