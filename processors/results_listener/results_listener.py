import logging

from commons.processor import Processor


class ResultsListener(Processor):
    def process(self, message):
        logging.info(f"action: results_listener | result: received_message | message: {message}")
        if message['content'][0] != "[":
            corr_id, _, content = message['content'].partition(",")
        else:
            content = message['content']
        return (message['corr_id'], f"{message['corr_id']},{content}")

    def finish_processing(self):
        # TODO: check
        pass
