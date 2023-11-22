import logging
from commons.processor import Processor, Response, ResponseType


class FilterConfig:
    def __init__(self, output_fields):
        self.output_fields = output_fields


class Filter(Processor):
    def __init__(self, config, client_id):
        self.config = config

    def process(self, message):
        logging.debug(f"Filtering message: {message}")
        filtered_message = {}
        for field in self.config.output_fields:
            filtered_message[field] = message[field]
        return Response(ResponseType.SINGLE, filtered_message)

    def finish_processing(self):
        pass
