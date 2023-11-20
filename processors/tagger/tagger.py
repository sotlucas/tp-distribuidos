from commons.processor import Processor


class Tagger(Processor):
    def __init__(self, tag_name):
        self.tag_name = tag_name

    def process(self, message):
        """
        Adds the tag name to the beginning of the message and sends it to the output.
        """
        return f"[{self.tag_name}]{message['payload']}"

    def finish_processing(self):
        pass
