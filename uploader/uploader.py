import logging
from communication import Communication


class Uploader:
    def __init__(self, communication_config, queue):
        self.queue = queue
        self.communication = Communication(communication_config)

    def start(self):
        while True:
            client_message = self.queue.get()
            self.communication.send_output(client_message)
            logging.info(f"action: message_upload | result: success")
