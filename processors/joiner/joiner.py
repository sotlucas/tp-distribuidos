from commons.processor import Processor, Response, ResponseType

LEG_ID = "legId"
STARTING_AIRPORT = "startingAirport"
DESTINATION_AIRPORT = "destinationAirport"
TOTAL_TRAVEL_DISTANCE = "totalTravelDistance"
STARTING_LATITUDE = "startingLatitude"
STARTING_LONGITUDE = "startingLongitude"
DESTINATION_LATITUDE = "destinationLatitude"
DESTINATION_LONGITUDE = "destinationLongitude"


class JoinerConfig:
    def __init__(self, state):
        self.state = state


class Joiner(Processor):
    def __init__(self, config, client_id):
        self.config = config
        self.client_id = client_id
        self.flights_cache = []
        self.all_airports_received = False
        self.airports = {}

    def process(self, message):
        # message fields: legId,startingAirport,destinationAirport,totalTravelDistance
        # output fields: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude

        if not self.all_airports_received:
            if not self.config.state.is_client_done(self.client_id):
                self.flights_cache.append(message)
                return
            self.all_airports_received = True

        if not self.airports:
            self.airports = self.config.state.get_airports(self.client_id)

        if self.flights_cache:
            # Process the cached messages
            messages = []
            for flight in self.flights_cache:
                messages.append(self.process_flight(flight))
            self.flights_cache = []

            # Process the message that triggered the cache
            messages.append(self.process_flight(message))
            
            return Response(ResponseType.MULTIPLE, messages)

        message = self.process_flight(message)
        return Response(ResponseType.SINGLE, message)

    def process_flight(self, flight):
        starting_airport = flight[STARTING_AIRPORT]
        destination_airport = flight[DESTINATION_AIRPORT]

        starting_airport_lat_long = self.airports[starting_airport]
        destination_airport_lat_long = self.airports[destination_airport]

        message = {
            LEG_ID: flight[LEG_ID],
            STARTING_AIRPORT: starting_airport,
            DESTINATION_AIRPORT: destination_airport,
            TOTAL_TRAVEL_DISTANCE: flight[TOTAL_TRAVEL_DISTANCE],
            STARTING_LATITUDE: starting_airport_lat_long[0],
            STARTING_LONGITUDE: starting_airport_lat_long[1],
            DESTINATION_LATITUDE: destination_airport_lat_long[0],
            DESTINATION_LONGITUDE: destination_airport_lat_long[1],
        }
        return message

    def finish_processing(self):
        pass
