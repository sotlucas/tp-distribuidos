class FilterConfig:
    def __init__(self, input_fields, output_fields, delimiter):
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.delimiter = delimiter


class Filter:
    def __init__(self, config, communication_receiver, communication_sender):
        self.config = config
        self.communication_receiver = communication_receiver
        self.communication_sender = communication_sender
        self.input_fields = config.input_fields.split(",")
        self.output_fields = config.output_fields.split(",")

    def run(self):
        self.communication_receiver.bind(
            input_callback=self.filter,
            eof_callback=self.communication_sender.send_eof,
            sender=self.communication_sender,
            input_fields_order=self.input_fields,
        )
        self.communication_receiver.start()

    def filter(self, messages):
        self.communication_sender.send_all(
            messages, output_fields_order=self.output_fields
        )
