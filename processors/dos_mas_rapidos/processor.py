import logging
import re
import signal

STARTING_AIRPORT_INDEX = 1
DESTINATION_AIRPORT_INDEX = 2
TRAVEL_DURATION_INDEX = 3


class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender
        self.trajectory = {}
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        self.receiver.bind(
            self.process, eof_callback=self.send_results, sender=self.sender
        )
        self.receiver.start()

    def process(self, messages):
        for message in messages:
            self.process_single(message)

    def process_single(self, message):
        """
        Checks if the travel duration is one of the two fastest and if it is,
        it adds the message to the fastest list
        """
        trajectory = self.convert_message_to_trajectory(message)
        if trajectory not in self.trajectory:
            self.trajectory[trajectory] = [message]
        else:
            self.add_to_fastest(self.trajectory[trajectory], message)

    def add_to_fastest(self, fastest, message):
        """
        Adds the message to the fastest list if the travel duration is one of the two fastest
        """
        travel_duration = self.convert_message_to_travel_duration(message)
        if len(fastest) < 2:
            fastest.append(message)
        else:
            second_fastest = self.convert_message_to_travel_duration(fastest[1])
            if travel_duration < second_fastest:
                fastest[1] = message
        fastest.sort(key=self.convert_message_to_travel_duration)

    def convert_message_to_trajectory(self, message):
        """
        Converts a message to a trajectory string
        """
        params = message.split(",")
        return params[STARTING_AIRPORT_INDEX] + "-" + params[DESTINATION_AIRPORT_INDEX]

    def convert_message_to_travel_duration(self, message):
        """
        Converts a message to a travel duration in minutes
        """
        params = message.split(",")
        return self.convert_travel_duration(params[TRAVEL_DURATION_INDEX])

    def convert_travel_duration(self, travel_duration):
        """
        Converts the travel duration from the ISO 8601 duration format to minutes.
        Example:
        PT1H30M -> 90
        P1DT8M -> 1448
        """
        hours = 0
        minutes = 0
        days = 0
        duration_match = re.search(
            r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?", travel_duration
        )
        if duration_match:
            days = int(duration_match.group(1) or 0)
            hours = int(duration_match.group(2) or 0)
            minutes = int(duration_match.group(3) or 0)
        return days * 24 * 60 + hours * 60 + minutes

    def send_results(self):
        """
        Sends the fastest messages to the output queue
        """
        logging.info("Sending results")
        # TODO: send in batch
        for trajectory in self.trajectory:
            for message in self.trajectory[trajectory]:
                self.sender.send(message)
        self.sender.send_eof()

    def __stop(self, *args):
        """
        Stop processor. Closing resources.
        """
        logging.info("action: processor_shutdown | result: in_progress")
        self.receiver.stop()
        self.sender.stop()
        logging.info("action: processor_shutdown | result: success")
