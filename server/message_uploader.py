import logging


class MessageUploader:
    """
    Sends messages to a queue.
    """

    def __init__(self, sender):
        self.sender = sender

    def send(self, message):
        self.sender.send_all(message)
        logging.debug(f"action: message_upload | result: success")

    def finish_sending(self):
        logging.info("Sending EOF")
        self.sender.send_eof()
        self.sender.close()

    def stop(self):
        """
        Stop message uploader closing resources.
        """
        logging.info("action: message_uploader_shutdown | result: in_progress")
        self.sender.stop()
        logging.info("action: message_uploader_shutdown | result: success")
