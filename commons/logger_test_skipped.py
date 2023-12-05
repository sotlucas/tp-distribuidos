import random

from commons.logger import Logger, RestoreType
from commons.processor import Processor


def _test_save():
    logger = Logger()
    processor = MockedProcessor()
    message = {
        "my_state": processor.my_state,
        "client_id": processor.client_id,
        "messages_received": processor.messages_received,
        "messages_sent": processor.messages_sent,
        "eof_current_id": processor.eof_current_id,
        "posible_duplicates_sent": processor.posible_duplicates_sent,
        "posible_duplicates_remaining": processor.posible_duplicates_remaining,
    }
    message_id = random.randint(1, 100)
    save_message_test_log_file(logger, message_id, processor.client_id, message)
    assert True


def _test_restore_from_commit():
    logger = Logger()
    expected_message_id = 81
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    save_message_test_log_file(
        logger, expected_message_id, expected_client_id, expected_state
    )

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == RestoreType.COMMIT
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state == expected_state


def _test_restore_from_save_done():
    logger = Logger()
    expected_message_id = 81
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    logger.start(expected_message_id, expected_client_id)
    logger.sent(expected_message_id, expected_client_id)
    logger.save(expected_message_id, expected_client_id, expected_state)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == RestoreType.SAVE_DONE
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state == expected_state


def _test_restore_from_sent_one_message_logged():
    logger = Logger()
    expected_message_id = 81
    expected_client_id = 10
    logger.start(expected_message_id, expected_client_id)
    logger.sent(expected_message_id, expected_client_id)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == RestoreType.SENT
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state is None


def _test_restore_from_sent_two_messages_logged():
    logger = Logger()
    # Save message 1
    expected_message_id = 44
    expected_client_id = 12
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    save_message_test_log_file(
        logger, expected_message_id, expected_client_id, expected_state
    )

    # Save message 2
    failed_message_id = 81
    failed_client_id = 10
    logger.start(failed_message_id, failed_client_id)
    logger.sent(failed_message_id, failed_client_id)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == RestoreType.SENT
    assert message_id == failed_message_id
    assert client_id == failed_client_id
    assert state == expected_state


def _test_restore_from_sent_two_uncommited_messages_in_a_row():
    logger = Logger()
    # Save message 1 - committed
    expected_message_id = 44
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    save_message_test_log_file(
        logger, expected_message_id, expected_client_id, expected_state
    )

    # Save message 2 - uncommited
    failed_message_id = 81
    failed_client_id = 10
    logger.start(failed_message_id, failed_client_id)
    logger.sent(failed_message_id, failed_client_id)

    # Save message 3 - uncommited
    failed_message_id = 99
    failed_client_id = 10
    logger.start(failed_message_id, failed_client_id)
    logger.sent(failed_message_id, failed_client_id)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == RestoreType.SENT
    assert message_id == failed_message_id
    assert client_id == failed_client_id
    assert state == expected_state


def _test_restore_empty_file():
    open("test_empty.txt", "w").close()
    logger = Logger("test_empty.txt")
    restore_type, message_id, client_id, state = logger.restore()
    assert restore_type is None
    assert message_id is None
    assert client_id is None
    assert state is None


def _test_restore_non_existent_file():
    logger = Logger("test_non_existent.txt")
    restore_type, message_id, client_id, state = logger.restore()
    assert restore_type is None
    assert message_id is None
    assert client_id is None
    assert state is None


def _test_search_processed():
    logger = Logger("search_processed.txt")
    expected_message_id = 81
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    logger.start(expected_message_id, expected_client_id)
    logger.sent(expected_message_id, expected_client_id)
    logger.save(expected_message_id, expected_client_id, expected_state)
    logger.commit(expected_message_id, expected_client_id)

    ids_to_search = [81, 82, 83]
    processed_ids_found = logger.search_processed(expected_client_id, ids_to_search)

    assert processed_ids_found == ["81S"]


def _test_search_processed_without_sent():
    logger = Logger("search_processed_without_sent.txt")
    expected_message_id = 83
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    logger.start(expected_message_id, expected_client_id)
    logger.save(expected_message_id, expected_client_id, expected_state)
    logger.commit(expected_message_id, expected_client_id)

    ids_to_search = [81, 82, 83]
    processed_ids_found = logger.search_processed(expected_client_id, ids_to_search)

    assert processed_ids_found == ["83"]


def _test_search_processed_many():
    logger = Logger("search_processed_many.txt")
    expected_message_id = 81
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    logger.start(expected_message_id, expected_client_id)
    logger.sent(expected_message_id, expected_client_id)
    logger.save(expected_message_id, expected_client_id, expected_state)
    logger.commit(expected_message_id, expected_client_id)

    expected_message_id = 83
    expected_client_id = 10
    expected_state = {
        "my_state": "my_state",
        "client_id": expected_client_id,
        "messages_received": 100,
        "messages_sent": 50,
        "eof_current_id": 0,
        "posible_duplicates_sent": None,
        "posible_duplicates_remaining": None,
    }
    logger.start(expected_message_id, expected_client_id)
    logger.save(expected_message_id, expected_client_id, expected_state)
    logger.commit(expected_message_id, expected_client_id)

    ids_to_search = [81, 82, 83]
    processed_ids_found = logger.search_processed(expected_client_id, ids_to_search)

    assert processed_ids_found == ["83", "81S"]


def _test_truncate():
    client_id = 1
    logger = Logger()
    logger.save_connection(1, client_id, "message1.1")
    logger.save_connection(2, client_id, "message1.2")
    logger.save_connection(3, client_id, "message2.3")
    logger.save_connection(4, client_id, "message2.4")
    logger.save_connection(5, client_id, "message2.5")
    logger.delete_connection_messages(5, client_id)
    assert True


_test_truncate()


def _test_truncate_does_not_delete_other_than_last():
    client_id = 1
    logger = Logger()
    logger.save_connection(1, client_id, "message1.1")
    logger.save_connection(2, client_id, "message1.2")
    logger.save_connection(3, client_id, "message2.3")
    logger.save_connection(4, client_id, "message2.4")
    logger.save_connection(5, client_id, "message2.5")
    logger.save_connection(6, client_id, "message2.6")
    logger.delete_connection_messages(5, client_id)
    assert True


# ---- Utils ----


class MockedProcessor(Processor):
    def __init__(self):
        super().__init__()
        self.my_state = "my_state"
        self.client_id = 10
        self.messages_received = 100
        self.messages_sent = 50
        self.eof_current_id = 0
        self.posible_duplicates_sent = None
        self.posible_duplicates_remaining = None

    def process(self, vuelos):
        pass


def save_message_test_log_file(logger, message_id, client_id, message):
    logger.start(message_id, client_id)
    logger.sent(message_id, client_id)
    logger.save(message_id, client_id, message)
    logger.commit(message_id, client_id)
