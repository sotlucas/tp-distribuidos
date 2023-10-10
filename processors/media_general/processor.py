import logging


class Processor:
    def __init__(self, communication):
        self.communication = communication
        self.media_general = 0

    def run(self):
        self.communication.run(input_callback=self.process)

    def process(self, message):
        # message = sum,amount
        prices_sum, amount = message.split(",")
        media_general = float(prices_sum) / int(amount)
        self.send_results(media_general)

    def send_results(self, media_general):
        logging.info("Sending results")
        self.communication.send_output(str(media_general))
