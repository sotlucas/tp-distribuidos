import csv


class FilterConfig:
    def __init__(self, input_fields, output_fields):
        self.input_fields = input_fields
        self.output_fields = output_fields


class Filter:
    def __init__(self, config):
        self.config = config

    def filter(self, message):
        input_fields = self.config.input_fields.split(",")
        reader = csv.DictReader([message], fieldnames=input_fields)
        for row in reader:
            print(row)
            output_fields = self.config.output_fields.split(",")
            filtered_row = [row[key] for key in output_fields]
            return ",".join(filtered_row)
