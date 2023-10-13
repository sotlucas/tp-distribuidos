import hashlib
import logging


class Processor:
    def __init__(self, grouper_replicas_count, receiver, sender):
        self.grouper_replicas_count = grouper_replicas_count
        self.receiver = receiver
        self.sender = sender

    def run(self):
        self.receiver.run(
            input_callback=self.process, eof_callback=self.sender.send_eof
        )

    def process(self, message):
        """
        Calculates the hash of the message and forwards it to the corresponding queue.
        """
        route = self.get_route(message)
        message_hash = hashlib.md5(route.encode()).hexdigest()
        queue_id = (int(message_hash, 16) % self.grouper_replicas_count) + 1
        logging.info(f"Forwarding message to queue {queue_id}")
        self.sender.send(message, str(queue_id))

    def get_route(self, message):
        starting_airport, destination_airport, _ = message.split(",")
        return f"{starting_airport}-{destination_airport}"
