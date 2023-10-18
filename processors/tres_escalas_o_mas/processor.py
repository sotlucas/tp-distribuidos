SEGMENTS_ARRIVAL_AIRPORT_CODE_INDEX = 5


class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.sender.send_eof,
            sender=self.sender,
        )
        self.receiver.start()

    def process(self, messages):
        processed = []
        for message in messages:
            processed_message = self.process_single(message)
            if processed_message:
                processed.append(processed_message)
        if len(processed) > 0:
            self.sender.send_all(processed)

    def process_single(self, message):
        params = message.split(",")
        segmentsArrivalAirportCode = params[SEGMENTS_ARRIVAL_AIRPORT_CODE_INDEX]
        arrivals = segmentsArrivalAirportCode.split("||")
        stopover = len(arrivals) - 1  # -1 because the last arrival is the destination
        if stopover >= 3:
            return message
        else:
            return None
