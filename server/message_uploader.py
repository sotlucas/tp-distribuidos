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
        logging.debug(f"action: message_upload {message.message_id} | result: success")

    def finish_sending(self, client_id, messages_sent, possible_duplicates):
        logging.info("Sending EOF")
        # Parse possible_duplicates list of str to int
        possible_duplicates = list(map(int, possible_duplicates))
        self.sender.send_eof(
            client_id,
            messages_sent=messages_sent,
            possible_duplicates=possible_duplicates,
        )
        self.sender.close()

    def stop(self):
        """
        Stop message uploader closing resources.
        """
        logging.info("action: message_uploader_shutdown | result: in_progress")
        self.sender.stop()
        logging.info("action: message_uploader_shutdown | result: success")
