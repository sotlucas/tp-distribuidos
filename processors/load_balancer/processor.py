import hashlib
import logging


class Processor:
    def __init__(self, grouper_replicas_count, receiver, sender):
        self.grouper_replicas_count = grouper_replicas_count
        self.receiver = receiver
        self.sender = sender

        self.input_output_fields = [
            "startingAirport",
            "destinationAirport",
            "totalFare",
        ]

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.send_eof_wrapper,
            sender=self.sender,
            input_fields_order=self.input_output_fields,
        )
        self.receiver.start()

    def process(self, messages):
        processed_messages = {}  # {queue_id: [messages]}
        for message in messages:
            message, queue_id = self.process_single(message)
            if queue_id not in processed_messages:
                processed_messages[queue_id] = []
            processed_messages[queue_id].append(message)
        for queue_id, messages_to_send in processed_messages.items():
            self.sender.send_all(
                messages_to_send,
                str(queue_id),
                output_fields_order=self.input_output_fields,
            )

    def process_single(self, message):
        """
        Calculates the hash of the message and the queue id to send it to
        """
        route = self.get_route(message)
        message_hash = hashlib.md5(route.encode()).hexdigest()
        queue_id = (int(message_hash, 16) % self.grouper_replicas_count) + 1
        logging.debug(f"Forwarding message to queue {queue_id}")
        return message, queue_id

    def get_route(self, message):
        starting_airport = message["startingAirport"]
        destination_airport = message["destinationAirport"]
        return f"{starting_airport}-{destination_airport}"

    def send_eof_wrapper(self):
        queue_id = 1  # send eof to the first grouper
        self.sender.send_eof(str(queue_id))
