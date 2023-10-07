import logging
from commons.communication import Communication

EOF = b"\0"


class FlightsUploader:
    def __init__(self, communication_config, queue):
        self.queue = queue
        self.communication = Communication(communication_config)
        self.running = True

    def start(self):
        while self.running:
            client_message = self.queue.get()
            if client_message == EOF:
                self.finish_sending()
            else:
                self.communication.send_output(client_message)
                logging.info(f"action: message_upload | result: success")
        self.communication.close()

    def finish_sending(self):
        logging.info("Sending EOF")
        self.running = False
        self.communication.send_eof()
