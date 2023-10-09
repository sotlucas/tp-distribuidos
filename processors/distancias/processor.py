from geopy.distance import geodesic


class Processor:
    def __init__(self, communication):
        self.communication = communication

    def run(self):
        self.communication.run(input_callback=self.process)

    def process(self, message):
        # input message: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude
        # output message: legId,startingAirport,destinationAirport,totalTravelDistance

        split_message = message.split(",")

        starting_latitude = split_message[4]
        starting_longitude = split_message[5]
        destination_latitude = split_message[6]
        destination_longitude = split_message[7]

        starting_airport = (starting_latitude, starting_longitude)
        destination_airport = (destination_latitude, destination_longitude)

        distance_between_airports = geodesic(
            starting_airport, destination_airport
        ).miles

        # We only send flights whose total distance is 4 times greater than the distance between airports
        total_distance = split_message[3]
        if not total_distance:
            # TODO: Verificar si hay que "skipear" los mensajes que no tienen distancia
            # If total distance is null in the database, we don't send the message
            return
        if float(total_distance) > 4 * distance_between_airports:
            self.communication.send_output(message)
