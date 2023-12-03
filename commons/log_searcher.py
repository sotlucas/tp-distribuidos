from commons.logger import Logger

class ProcessedMessage:
    def __init__(self, message_id, sent):
        self.message_id = message_id
        self.sent = sent

    def from_str(message_str):
        sent = "S" in message_str
        message_id = str.split("S")[0]
        return ProcessedMessage(message_id, sent)

    def to_str(self):
        return f"{self.message_id}S" if self.sent else self.message_id

    def __eq__(self, other):
        return self.message_id == other.message_id and self.sent == other.sent

class LogSearcher:
    def __init__(self, suffix=""):
        self.logger = Logger(suffix)

    def search_for_duplicate_messages(self, client_id, ids_to_search):
        """
        Searches for duplicate messages in the log file.
        """
        messages = self.logger.search_processed(client_id, ids_to_search)
        return [ProcessedMessage.from_str(message) for message in messages]
