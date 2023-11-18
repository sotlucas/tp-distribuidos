import logging
from commons.processor import Processor


class FilterConfig:
    def __init__(self, output_fields):
        self.output_fields = output_fields


class Filter(Processor):
    def __init__(self, config):
        self.config = config

    def process(self, message):
        filtered_message = {}
        for field in self.config.output_fields:
            filtered_message[field] = message[field]
        return filtered_message

    def finish_processing(self):
        pass