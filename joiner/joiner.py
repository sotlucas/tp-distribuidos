import logging


class Joiner:
    def __init__(self, communication_lat_long, communication_distancia):
        self.communication_lat_long = communication_lat_long
        self.communication_distancia = communication_distancia
        self.lat_long_airports = {}

    def run(self):
        self.communication_lat_long.run(
            input_callback=self.save_lat_long_airport, eof_callback=self.start_joining
        )

    def save_lat_long_airport(self, message):
        # message fields: AirportCode,Latitude,Longitude
        airport_code, latitude, longitude = message.split(",")
        self.lat_long_airports[airport_code] = (latitude, longitude)

    def start_joining(self):
        logging.info("Starting joining")
        self.communication_distancia.run(input_callback=self.join_lat_long_airport)

    def join_lat_long_airport(self, message):
        # message fields: legId,startingAirport,destinationAirport,totalTravelDistance
        # output fields: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude

        split_message = message.split(",")
        starting_airport = split_message[1]
        destination_airport = split_message[2]

        starting_airport_lat_long = self.lat_long_airports[starting_airport]
        destination_airport_lat_long = self.lat_long_airports[destination_airport]

        output_message = ",".join(
            [message, *starting_airport_lat_long, *destination_airport_lat_long]
        )

        self.communication_distancia.send_output(output_message)
