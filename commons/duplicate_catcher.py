import logging


class DuplicateCatcher:
    def __init__(self, initial_messages_id=None):
        self.messages_id = set(initial_messages_id) if initial_messages_id else set()

    def is_duplicate(self, message_id):
        """
        Checks if the message is a duplicate. If it is not, it is added to the set of messages and returned.
        """
        if message_id in self.messages_id:
            logging.info(f"Duplicate message {message_id} received.")
            return True
        self.messages_id.add(message_id)

        return False

    def get_state(self):
        return list(self.messages_id)
