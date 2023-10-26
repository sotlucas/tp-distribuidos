import logging
import signal

SEGMENTS_ARRIVAL_AIRPORT = "segmentsArrivalAirportCode"


class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender
        self.input_output_fields = [
            "legId",
            "startingAirport",
            "destinationAirport",
            "totalFare",
            "travelDuration",
            "segmentsArrivalAirportCode",
        ]
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.sender.send_eof,
            sender=self.sender,
            input_fields_order=self.input_output_fields,
        )
        self.receiver.start()

    def process(self, messages):
        processed = []
        for message in messages:
            processed_message = self.process_single(message)
            if processed_message:
                processed.append(processed_message)
        self.sender.send_all(processed, output_fields_order=self.input_output_fields)

    def process_single(self, message):
        segmentsArrivalAirportCode = message[SEGMENTS_ARRIVAL_AIRPORT]
        arrivals = segmentsArrivalAirportCode.split("||")
        stopover = len(arrivals) - 1  # -1 because the last arrival is the destination
        if stopover >= 3:
            return message
        else:
            return None

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: processor_shutdown | result: in_progress")
        self.receiver.stop()
        self.sender.stop()
        logging.info("action: processor_shutdown | result: success")
