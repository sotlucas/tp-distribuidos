from commons.log_searcher import LogSearcher
from commons.log_storer import LogStorer
from commons.restorer import Restorer


class LogGuardian:
    def __init__(self, log_suffix="", no_log=False):
        # no_log is only only by the server.

        self.restorer = Restorer(log_suffix) if not no_log else None
        self.storer = LogStorer(log_suffix) if not no_log else None
        self.searcher = LogSearcher(log_suffix) if not no_log else None

    def get_messages_received(self):
        if not self.restorer:
            return {}
        return self.restorer.get_messages_received()

    def get_messages_sent(self):
        if not self.restorer:
            return {}
        return self.restorer.get_messages_sent()

    def get_possible_duplicates(self):
        if not self.restorer:
            return {}
        return self.restorer.get_possible_duplicates()

    # ------------------------------STORER------------------------------

    def new_message_received(self, message_id, client_id):
        if not self.storer:
            return
        self.storer.new_message_received(message_id, client_id)

    def message_sent(self):
        if not self.storer:
            return
        self.storer.message_sent()

    def store_messages_received(self, messages_received):
        if not self.storer:
            return
        self.storer.store_messages_received(messages_received)

    def store_messages_sent(self, messages_sent):
        if not self.storer:
            return
        self.storer.store_messages_sent(messages_sent)

    def store_possible_duplicates(self, possible_duplicates):
        if not self.storer:
            return
        self.storer.store_possible_duplicates(possible_duplicates)

    def store_new_message_for_duplicate_catcher(self):
        if not self.storer:
            return
        self.storer.store_new_message_for_duplicate_catcher()

    def store_new_connection_message(self, message):
        if not self.storer:
            return
        self.storer.store_new_connection_message(message)

    def finish_storing_message(self):
        if not self.storer:
            return
        self.storer.finish_storing_message()

    def commit_message(self):
        if not self.storer:
            return
        self.storer.commit_message()

    # ------------------------------SEARCHER------------------------------

    def search_for_duplicate_messages(self, client_id, ids_to_search):
        if not self.storer:
            return []
        return self.searcher.search_for_duplicate_messages(client_id, ids_to_search)

    def search_for_all_connection_messages(self, client_id):
        if not self.storer:
            return []
        return self.searcher.search_for_all_connection_messages(client_id)

    def obtain_all_active_connection_clients(self):
        if not self.storer:
            return []
        return self.searcher.obtain_all_active_connection_clients()

    def search_for_all_duplicate_catcher_messages(self, client_id):
        if not self.storer:
            return []
        return self.searcher.search_for_all_duplicate_catcher_messages(client_id)

    def obtain_all_active_duplicate_catcher_clients(self):
        if not self.storer:
            return []
        return self.searcher.obtain_all_active_duplicate_catcher_clients()
