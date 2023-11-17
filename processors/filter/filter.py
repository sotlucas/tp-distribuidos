import logging
from commons.processor import Processor


class FilterConfig:
    def __init__(self, output_fields):
        self.output_fields = output_fields


class Filter(Processor):
    def __init__(self, config):
        self.config = config

    def process(self, message):
        logging.debug(f"Filtering message: {message}")
        filtered_message = {}
        for field in self.config.output_fields:
            filtered_message[field] = message['content'][field]
        return {**filtered_message, 'corr_id': message['corr_id']}

    def finish_processing(self):
        pass
