from geopy.distance import geodesic

from commons.processor import Processor


class Distancias(Processor):
    def __init__(self):
        self.cache = {}

    def process(self, message):
        # input message: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude
        # output message: legId,startingAirport,destinationAirport,totalTravelDistance

        starting_latitude = message["startingLatitude"]
        starting_longitude = message["startingLongitude"]
        destination_latitude = message["destinationLatitude"]
        destination_longitude = message["destinationLongitude"]

        starting_airport = (starting_latitude, starting_longitude)
        destination_airport = (destination_latitude, destination_longitude)
        distance_between_airports = self.distance(destination_airport, starting_airport)

        # We only send flights whose total distance is 4 times greater than the distance between airports
        total_distance = message["totalTravelDistance"]
        if not total_distance:
            # TODO: Verificar si hay que "skipear" los mensajes que no tienen distancia
            # If total distance is null in the database, we don't send the message
            return None
        if float(total_distance) > 4 * distance_between_airports:
            message = {
                "legId": message["legId"],
                "startingAirport": message["startingAirport"],
                "destinationAirport": message["destinationAirport"],
                "totalTravelDistance": message["totalTravelDistance"],
            }
            return message

    def distance(self, destination_airport, starting_airport):
        """
        Calculates the distance between two airports
        """
        # First we check if we already have the distance between the airports in the cache
        if (starting_airport, destination_airport) in self.cache:
            distance_between_airports = self.cache[
                (starting_airport, destination_airport)
            ]
        else:
            # If we don't have it, we calculate it and add it to the cache
            distance_between_airports = geodesic(
                starting_airport, destination_airport
            ).miles
            self.cache[
                (starting_airport, destination_airport)
            ] = distance_between_airports
        return distance_between_airports

    def finish_processing(self):
        pass
