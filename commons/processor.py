class Processor:
    def process(self, message):
        raise NotImplementedError(
            "process method is not implemented, subclass must implement it"
        )

    def finish_processing(self, client_id):
        raise NotImplementedError(
            "finish_processing method is not implemented, subclass must implement it"
        )
