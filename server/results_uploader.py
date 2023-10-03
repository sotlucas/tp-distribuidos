import logging
from commons.communication import Communication
from commons.protocol import END_OF_MESSAGE


class ResultsUploader:
    def __init__(self, communication_config, socket):
        self.socket = socket
        self.communication = Communication(communication_config)

    def start(self):
        logging.info(f"action: results_uploader | result: success ")
        self.communication.run(output_callback=self.output_callback)

    def output_callback(self, message):
        # Add the END_OF_MESSAGE sequence to mark the end of the message
        message_bytes = message.encode() + END_OF_MESSAGE
        self.socket.sendall(message_bytes)
        logging.info(f"action: result_upload | result: success")
