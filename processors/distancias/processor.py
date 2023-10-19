from geopy.distance import geodesic


class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender
        self.cache = {}

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.sender.send_eof,
            sender=self.sender,
        )
        self.receiver.start()

    def process(self, messages):
        processed_messages = []
        for message in messages:
            processed_message = self.process_single(message)
            if processed_message:
                processed_messages.append(processed_message)
        if len(processed_messages) > 0:
            self.sender.send_all(processed_messages)

    def process_single(self, message):
        # input message: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude
        # output message: legId,startingAirport,destinationAirport,totalTravelDistance

        split_message = message.split(",")

        starting_latitude = split_message[4]
        starting_longitude = split_message[5]
        destination_latitude = split_message[6]
        destination_longitude = split_message[7]

        starting_airport = (starting_latitude, starting_longitude)
        destination_airport = (destination_latitude, destination_longitude)
        distance_between_airports = self.distance(destination_airport, starting_airport)

        # We only send flights whose total distance is 4 times greater than the distance between airports
        total_distance = split_message[3]
        if not total_distance:
            # TODO: Verificar si hay que "skipear" los mensajes que no tienen distancia
            # If total distance is null in the database, we don't send the message
            return None
        if float(total_distance) > 4 * distance_between_airports:
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
            self.cache[(starting_airport, destination_airport)] = distance_between_airports
        return distance_between_airports
