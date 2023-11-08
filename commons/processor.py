class Processor:
    def __init__(self, config):
        raise NotImplementedError(
            "init method is not implemented, subclass must implement it"
        )

    def process(self, message):
        raise NotImplementedError(
            "process method is not implemented, subclass must implement it"
        )

    def finish_processing(self):
        raise NotImplementedError(
            "finish_processing method is not implemented, subclass must implement it"
        )
