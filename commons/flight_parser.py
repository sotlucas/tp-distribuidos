class FlightParser:
    def __init__(self, delimeter):
        self.delimeter = delimeter

    def parse(self, message_string, input_fields):
        """
        Parse the message and return a dict with the input fields
        """
        return dict(zip(input_fields, message_string.split(self.delimeter)))

    def serialize(self, message_dict, output_fields):
        """
        Serialize the message and return a string with the output fields
        """
        return ",".join([str(message_dict[key]) for key in output_fields])
