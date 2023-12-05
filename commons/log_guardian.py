from commons.log_searcher import LogSearcher
from commons.log_storer import LogStorer
from commons.restorer import Restorer


class LogGuardian:
    def __init__(self, log_suffix=""):
        self.restorer = Restorer(log_suffix)
        self.storer = LogStorer(log_suffix)
        self.searcher = LogSearcher(log_suffix)

    def get_messages_received(self):
        return self.restorer.get_messages_received()

    def get_messages_sent(self):
        return self.restorer.get_messages_sent()

    def get_possible_duplicates(self):
        return self.restorer.get_possible_duplicates()

    def get_duplicate_catchers(self):
        return self.restorer.get_duplicate_catchers()

    # ------------------------------STORER------------------------------

    def new_message_received(self, message_id, client_id):
        self.storer.new_message_received(message_id, client_id)

    def message_sent(self):
        self.storer.message_sent()

    def store_messages_received(self, messages_received):
        self.storer.store_messages_received(messages_received)

    def store_messages_sent(self, messages_sent):
        self.storer.store_messages_sent(messages_sent)

    def store_possible_duplicates(self, possible_duplicates):
        self.storer.store_possible_duplicates(possible_duplicates)

    def store_duplicate_catchers(self, duplicate_catchers):
        self.storer.store_duplicate_catchers(duplicate_catchers)

    def store_new_connection_message(self, message):
        self.storer.store_new_connection_message(message)

    def finish_storing_message(self):
        self.storer.finish_storing_message()

    def commit_message(self):
        self.storer.commit_message()

    # ------------------------------SEARCHER------------------------------

    def search_for_duplicate_messages(self, client_id, ids_to_search):
        return self.searcher.search_for_duplicate_messages(client_id, ids_to_search)

    def search_for_all_connection_messages(self, client_id):
        return self.searcher.search_for_all_connection_messages(client_id)

    def obtain_all_active_clients(self):
        return self.searcher.obtain_all_active_clients()
