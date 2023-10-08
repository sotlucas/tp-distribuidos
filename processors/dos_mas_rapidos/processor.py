import logging

STARTING_AIRPORT_INDEX = 1
DESTINATION_AIRPORT_INDEX = 2
TRAVEL_DURATION_INDEX = 3


class Processor:
    def __init__(self, communication):
        self.communication = communication
        self.trajectory = {}

    def run(self):
        self.communication.run(self.proccess, eof_callback=self.send_results)

    def proccess(self, message):
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
        Converts the travel duration to minutes.
        The input format is PT1H30M and the output format is 90
        """
        # TODO: consider days
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
        for trajectory in self.trajectory:
            for message in self.trajectory[trajectory]:
                self.communication.send_output(message)
        self.communication.send_eof()
