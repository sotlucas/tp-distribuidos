from commons.processor import Processor


class Joiner(Processor):
    def __init__(self, lat_long_airports):
        self.lat_long_airports = lat_long_airports

    def process(self, message):
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

    def finish_processing(self):
        pass
