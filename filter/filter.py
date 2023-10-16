import csv


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

    def run(self):
        self.communication_receiver.bind(
            input_callback=self.filter, eof_callback=self.communication_sender.send_eof
        )
        self.communication_receiver.start()

    def filter(self, messages):
        input_fields = self.config.input_fields.split(",")
        reader = csv.DictReader(messages, fieldnames=input_fields, delimiter=self.config.delimiter)
        rows = []
        for row in reader:
            output_fields = self.config.output_fields.split(",")
            filtered_row = [row[key] for key in output_fields]
            rows.append(",".join(filtered_row))
        self.communication_sender.send_all(rows)
