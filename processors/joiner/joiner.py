import logging
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
        self.airports = {}

    def process(self, message):
        # message fields: legId,startingAirport,destinationAirport,totalTravelDistance
        # output fields: legId,startingAirport,destinationAirport,totalTravelDistance,startingLatitude,startingLongitude,destinationLatitude,destinationLongitude

        starting_airport_code = message[STARTING_AIRPORT]
        destination_airport_code = message[DESTINATION_AIRPORT]

        starting_airport_lat_long = self.obtain_airport_lat_long(starting_airport_code)
        destination_airport_lat_long = self.obtain_airport_lat_long(
            destination_airport_code
        )

        if not starting_airport_lat_long or not destination_airport_lat_long:
            # We are not ready to process the message, we need to wait for all the airports
            logging.debug(
                "Joiner {} not ready to process messages".format(self.client_id)
            )
            return Response(ResponseType.NOT_READY, None)

        message = self.process_flight(
            message,
            starting_airport_code,
            starting_airport_lat_long,
            destination_airport_code,
            destination_airport_lat_long,
        )
        return Response(ResponseType.SINGLE, message)

    def obtain_airport_lat_long(self, airport_code):
        if not airport_code in self.airports:
            airport = self.config.state.obtain_client_airport(
                self.client_id, airport_code
            )
            if not airport:
                return None
            self.airports[airport_code] = airport
        return self.airports[airport_code]

    def process_flight(
        self,
        flight,
        starting_airport_code,
        starting_airport_lat_long,
        destination_airport_code,
        destination_airport_lat_long,
    ):
        message = {
            LEG_ID: flight[LEG_ID],
            STARTING_AIRPORT: starting_airport_code,
            DESTINATION_AIRPORT: destination_airport_code,
            TOTAL_TRAVEL_DISTANCE: flight[TOTAL_TRAVEL_DISTANCE],
            STARTING_LATITUDE: starting_airport_lat_long[0],
            STARTING_LONGITUDE: starting_airport_lat_long[1],
            DESTINATION_LATITUDE: destination_airport_lat_long[0],
            DESTINATION_LONGITUDE: destination_airport_lat_long[1],
        }
        return message

    def finish_processing(self):
        pass
