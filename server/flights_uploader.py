import logging


class FlightsUploader:
    def __init__(self, sender):
        self.sender = sender

    def send(self, client_message):
        self.sender.send(client_message)
        logging.info(f"action: message_upload | result: success")

    def finish_sending(self):
        logging.info("Sending EOF")
        self.sender.send_eof()
        self.sender.close()
