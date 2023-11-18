import logging

from commons.processor import Processor


class ResultsListener(Processor):
    def process(self, message):
        logging.info(f"action: results_listener | result: received_message | message: {message}")
        return (message.client_id, message.payload_bytes)

    def finish_processing(self):
        # TODO: check
        pass
