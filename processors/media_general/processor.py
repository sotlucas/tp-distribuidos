import logging
from time import sleep


class Processor:
    def __init__(self, grouper_replicas_count, receiver, sender):
        self.grouper_replicas_count = grouper_replicas_count
        self.receiver = receiver
        self.sender = sender
        self.amount_received = 0  # Number of groupers that have sent their results
        self.price_sum = 0
        self.amount = 0
        self.media_general = 0

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.send_results,
            sender=self.sender,
        )
        self.receiver.start()

    def process(self, messages):
        for message in messages:
            self.process_single(message)

    def process_single(self, message):
        # message = sum,amount
        prices_sum, amount = message.split(",")
        self.price_sum += float(prices_sum)
        self.amount += int(amount)
        self.amount_received += 1
        if self.amount_received == self.grouper_replicas_count:
            media_general = self.price_sum / self.amount
            self.send_results(media_general)

    def send_results(self, media_general):
        logging.info("Sending results")
        self.sender.send(str(media_general))
        # self.sender.send_eof()
