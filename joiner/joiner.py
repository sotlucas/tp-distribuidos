import logging


class Joiner:
    def __init__(self, lat_long_receiver, vuelos_receiver, vuelos_sender):
        self.lat_long_receiver = lat_long_receiver
        self.vuelos_receiver = vuelos_receiver
        self.vuelos_sender = vuelos_sender
        self.lat_long_airports = {}

        self.lat_long_input_fields = ["AirportCode", "Latitude", "Longitude"]
        self.vuelos_input_fields = [
            "legId",
            "startingAirport",
            "destinationAirport",
            "totalTravelDistance",
        ]
        self.vuelos_output_fields = [
            "legId",
            "startingAirport",
            "destinationAirport",
            "totalTravelDistance",
            "startingLatitude",
            "startingLongitude",
            "destinationLatitude",
            "destinationLongitude",
        ]

    def run(self):
        self.lat_long_receiver.bind(
            input_callback=self.save_lat_long_airports,
            eof_callback=self.start_joining,
            input_fields_order=self.lat_long_input_fields,
        )
        self.lat_long_receiver.start()

    def save_lat_long_airports(self, messages):
        for message in messages:
            self.save_lat_long_airport(message)

    def save_lat_long_airport(self, message):
        # message fields: AirportCode,Latitude,Longitude
        airport_code, latitude, longitude = (
            message["AirportCode"],
            message["Latitude"],
            message["Longitude"],
        )
        self.lat_long_airports[airport_code] = (latitude, longitude)

    def start_joining(self):
        logging.info("Starting joining")
        self.vuelos_receiver.bind(
            input_callback=self.join_lat_long_airports,
            eof_callback=self.vuelos_sender.send_eof,
            sender=self.vuelos_sender,
            input_fields_order=self.vuelos_input_fields,
        )
        self.vuelos_receiver.start()

    def join_lat_long_airports(self, messages):
        joined_messages = []
        for message in messages:
            joined_messages.append(self.join_lat_long_airport(message))
        self.vuelos_sender.send_all(joined_messages, self.vuelos_output_fields)

    def join_lat_long_airport(self, message):
        # message fields: legId,startingAirport,destinationAirport,totalTravelDistance
        # output fields: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude

        starting_airport = message["startingAirport"]
        destination_airport = message["destinationAirport"]

        starting_airport_lat_long = self.lat_long_airports[starting_airport]
        destination_airport_lat_long = self.lat_long_airports[destination_airport]

        message = {
            "legId": message["legId"],
            "startingAirport": starting_airport,
            "destinationAirport": destination_airport,
            "totalTravelDistance": message["totalTravelDistance"],
            "startingLatitude": starting_airport_lat_long[0],
            "startingLongitude": starting_airport_lat_long[1],
            "destinationLatitude": destination_airport_lat_long[0],
            "destinationLongitude": destination_airport_lat_long[1],
        }
        return message
