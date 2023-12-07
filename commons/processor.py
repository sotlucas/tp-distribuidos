from enum import Enum


class ResponseType(Enum):
    SINGLE = 0
    MULTIPLE = 1
    NOT_READY = 2  # Only used for joiner processor
    SEND_EOF = 3  # Only used for max_avg processor


class Response:
    def __init__(self, response_type, payload):
        self.type = response_type
        self.payload = payload


class Processor:
    def process(self, message):
        raise NotImplementedError(
            "process method is not implemented, subclass must implement it"
        )

    def finish_processing(self):
        raise NotImplementedError(
            "finish_processing method is not implemented, subclass must implement it"
        )
