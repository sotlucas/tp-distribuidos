import random

from commons.logger import Logger
from commons.processor import Processor


def test_save():
    logger = Logger("test.txt")
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


def test_restore_from_commit():
    logger = Logger("test_commit.txt")
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
    save_message_test_log_file(logger, expected_message_id, expected_client_id, expected_state)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == "COMMIT"
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state == expected_state


def test_restore_from_save_done():
    logger = Logger("test_save_done.txt")
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

    assert restore_type == "SAVE DONE"
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state == expected_state


def test_restore_from_sent_one_message_logged():
    logger = Logger("test_sent.txt")
    expected_message_id = 81
    expected_client_id = 10
    logger.start(expected_message_id, expected_client_id)
    logger.sent(expected_message_id, expected_client_id)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == "SENT"
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state is None


def test_restore_from_sent_two_messages_logged():
    logger = Logger("test_sent_two.txt")
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
    save_message_test_log_file(logger, expected_message_id, expected_client_id, expected_state)

    # Save message 2
    failed_message_id = 81
    failed_client_id = 10
    logger.start(failed_message_id, failed_client_id)
    logger.sent(failed_message_id, failed_client_id)

    restore_type, message_id, client_id, state = logger.restore()

    assert restore_type == "SENT"
    assert message_id == expected_message_id
    assert client_id == expected_client_id
    assert state == expected_state


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
