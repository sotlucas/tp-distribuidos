import logging
from commons.processor import Processor


class MediaGeneralConfig:
    def __init__(self, grouper_replicas_count):
        self.grouper_replicas_count = grouper_replicas_count


class MediaGeneral(Processor):
    def __init__(self, config):
        self.config = config
        self.amount_received = 0  # Number of groupers that have sent their results
        self.price_sum = 0
        self.amount = 0
        self.media_general = 0

    def process(self, message):
        # message = sum,amount
        prices_sum, amount = message["sum"], message["amount"]
        self.price_sum += float(prices_sum)
        self.amount += int(amount)
        self.amount_received += 1
        if self.amount_received == self.config.grouper_replicas_count:
            logging.info("received all messages, calculating media")
            media_general = self.price_sum / self.amount
            message = {"media_general": str(media_general)}
            return message

    def finish_processing(self):
        pass
