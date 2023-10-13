SEGMENTS_ARRIVAL_AIRPORT_CODE_INDEX = 5


class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender

    def run(self):
        self.receiver.run(
            input_callback=self.proccess, eof_callback=self.sender.send_eof
        )

    def proccess(self, message):
        params = message.split(",")
        segmentsArrivalAirportCode = params[SEGMENTS_ARRIVAL_AIRPORT_CODE_INDEX]
        arrivals = segmentsArrivalAirportCode.split("||")
        stopover = len(arrivals) - 1  # -1 because the last arrival is the destination
        if stopover >= 3:
            self.sender.send(message)
        else:
            return None
