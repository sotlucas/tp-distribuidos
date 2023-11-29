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
    logger.start(message_id, processor.client_id)
    logger.sent(message_id, processor.client_id)
    logger.save(message_id, processor.client_id, message)
    logger.commit(message_id, processor.client_id)
    assert True


def test_read():
    logger = Logger("test.txt")
    logger.restore()
    assert True


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
