import logging
from commons.communication import Communication

EOF = b"\0"


class FlightsUploader:
    def __init__(self, sender, queue):
        self.queue = queue
        self.sender = sender
        self.running = True

    def start(self):
        while self.running:
            client_message = self.queue.get()
            if client_message == EOF:
                self.finish_sending()
            else:
                self.sender.send(client_message)
                logging.info(f"action: message_upload | result: success")
        self.sender.close()

    def finish_sending(self):
        logging.info("Sending EOF")
        self.running = False
        self.sender.send_eof()
