import logging
from commons.message import ProtocolMessage

# TODO: remove this
TEST_ID = 0


class MessageUploader:
    """
    Sends messages to a queue.
    """

    def __init__(self, sender):
        self.sender = sender

    def send(self, client_message):
        # TODO: Decoding here because send needs a string, find a better way
        #       And it is a list because send needs a list, find a better way
        message = ProtocolMessage(TEST_ID, [client_message.decode("utf-8")])
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
