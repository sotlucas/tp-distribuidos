import logging


class Joiner:
    def __init__(self, lat_long_receiver, vuelos_receiver, vuelos_sender):
        self.lat_long_receiver = lat_long_receiver
        self.vuelos_receiver = vuelos_receiver
        self.vuelos_sender = vuelos_sender
        self.lat_long_airports = {}

    def run(self):
        self.lat_long_receiver.bind(
            input_callback=self.save_lat_long_airports, eof_callback=self.start_joining
        )
        self.lat_long_receiver.start()

    def save_lat_long_airports(self, messages):
        for message in messages:
            self.save_lat_long_airport(message)

    def save_lat_long_airport(self, message):
        # message fields: AirportCode,Latitude,Longitude
        airport_code, latitude, longitude = message.split(",")
        self.lat_long_airports[airport_code] = (latitude, longitude)

    def start_joining(self):
        logging.info("Starting joining")
        self.vuelos_receiver.bind(
            input_callback=self.join_lat_long_airports,
            eof_callback=self.vuelos_sender.send_eof,
            sender=self.vuelos_sender,
        )
        self.vuelos_receiver.start()

    def join_lat_long_airports(self, messages):
        joined_messages = []
        for message in messages:
            joined_messages.append(self.join_lat_long_airport(message))
        if len(joined_messages) > 0:
            self.vuelos_sender.send_all(joined_messages)

    def join_lat_long_airport(self, message):
        # message fields: legId,startingAirport,destinationAirport,totalTravelDistance
        # output fields: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude

        split_message = message.split(",")
        starting_airport = split_message[1]
        destination_airport = split_message[2]

        starting_airport_lat_long = self.lat_long_airports[starting_airport]
        destination_airport_lat_long = self.lat_long_airports[destination_airport]

        return ",".join(
            [message, *starting_airport_lat_long, *destination_airport_lat_long]
        )
