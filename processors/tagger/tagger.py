from commons.processor import Processor


class TaggerConfig:
    def __init__(self, tag_name):
        self.tag_name = tag_name


class Tagger(Processor):
    def __init__(self, config):
        self.config = config

    def process(self, message):
        """
        Adds the tag name to the beginning of the message and sends it to the output.
        """
        return f"[{self.config.tag_name}]{message}"

    def finish_processing(self):
        pass
