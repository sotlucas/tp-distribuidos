from commons.logger import Logger


class LogStorer:
    def __init__(self, suffix=""):
        self.logger = Logger(suffix)
        self.current_client_id = None
        self.current_message_id = None
        self.current_state = {}
        self.connection_messages_state = []
        self.new_message_for_duplicate_catcher = False

    def new_message_received(self, message_id, client_id):
        self.current_state = {}
        self.connection_messages_state = []
        self.new_message_for_duplicate_catcher = False
        self.current_message_id = message_id
        self.current_client_id = client_id

        self.logger.start(message_id, client_id)

    def message_sent(self):
        self.logger.sent(self.current_message_id, self.current_client_id)

    def store_messages_received(self, messages_received):
        self.current_state["messages_received"] = messages_received

    def store_messages_sent(self, messages_sent):
        self.current_state["messages_sent"] = messages_sent

    def store_possible_duplicates(self, possible_duplicates):
        self.current_state["possible_duplicates"] = possible_duplicates

    def store_new_message_for_duplicate_catcher(self):
        self.new_message_for_duplicate_catcher = True

    def store_new_connection_message(self, message):
        self.connection_messages_state = message

    def finish_storing_message(self):
        if self.connection_messages_state:
            self.logger.save_connection(
                self.current_message_id,
                self.current_client_id,
                self.connection_messages_state,
            )

        if self.new_message_for_duplicate_catcher:
            self.logger.save_duplicate_catcher(
                self.current_message_id,
                self.current_client_id,
            )

        self.logger.save_communication(
            self.current_message_id,
            self.current_client_id,
            self.current_state,
        )

    def commit_message(self):
        self.logger.commit(self.current_message_id, self.current_client_id)
