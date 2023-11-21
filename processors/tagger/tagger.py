from commons.processor import Processor, Respose, ResponseType


class TaggerConfig:
    def __init__(self, tag_name):
        self.tag_name = tag_name


class Tagger(Processor):
    def __init__(self, config, client_id):
        self.config = config

    def process(self, message):
        """
        Adds the tag name to the beginning of the message and sends it to the output.
        """
        message = f"[{self.config.tag_name}]{message}"
        return Respose(ResponseType.SINGLE, message)

    def finish_processing(self):
        pass
