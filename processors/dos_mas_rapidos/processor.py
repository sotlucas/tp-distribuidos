import logging

TRAVEL_DURATION_INDEX = 3


class Processor:
    def __init__(self, communication):
        self.communication = communication
        # TODO: group by trajectory
        self.fastest = []

    def run(self):
        self.communication.run(self.proccess, eof_callback=self.send_results)

    def proccess(self, message):
        """
        Checks if the travel duration is one of the two fastest and if it is,
        it adds the message to the fastest list
        """
        travel_duration = self.convert_message_to_travel_duration(message)
        if len(self.fastest) < 2:
            self.fastest.append(message)
        else:
            second_fastest = self.convert_message_to_travel_duration(self.fastest[1])
            if travel_duration < second_fastest:
                self.fastest[1] = message
        self.fastest.sort(key=self.convert_message_to_travel_duration)

    def convert_message_to_travel_duration(self, message):
        """
        Converts a message to a travel duration in minutes
        """
        params = message.split(",")
        return self.convert_travel_duration(params[TRAVEL_DURATION_INDEX])

    def convert_travel_duration(self, travel_duration):
        """
        Converts the travel duration to minutes.
        The input format is PT1H30M and the output format is 90
        """
        hours = 0
        minutes = 0
        if "H" in travel_duration:
            hours = int(travel_duration.split("H")[0].split("T")[1])
        if "M" in travel_duration:
            minutes = int(travel_duration.split("M")[0].split("H")[1])
        return hours * 60 + minutes

    def send_results(self):
        """
        Sends the fastest messages to the output queue
        """
        logging.info("Sending results")
        for message in self.fastest:
            self.communication.send_output(message)
        self.communication.send_eof()
