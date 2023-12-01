import logging

from commons.communication import CommunicationSender


class MessageUploader:
    """
    Sends messages to a queue.
    """

    def __init__(self, sender: CommunicationSender):
        self.sender = sender

    def send(self, message):
        self.sender.send_all(message)
        logging.debug(f"action: message_upload | result: success")

    def finish_sending(self, client_id):
        logging.info("Sending EOF")
        self.sender.send_eof(client_id)
        self.sender.close()

    def stop(self):
        """
        Stop message uploader closing resources.
        """
        logging.info("action: message_uploader_shutdown | result: in_progress")
        self.sender.stop()
        logging.info("action: message_uploader_shutdown | result: success")
