import logging


# TODO: this is the same as FlightsUploader
class LatLongUploader:
    def __init__(self, sender):
        self.sender = sender

    def send(self, client_message):
        self.sender.send(client_message)
        logging.debug(f"action: message_upload | result: success")

    def finish_sending(self):
        logging.info("LATLONG UPLOADER::Sending EOF")
        self.sender.send_eof()
        self.sender.close()
