from commons.processor import Processor
from commons.protocol import EOF


class LatLong(Processor):
    def __init__(self):
        self.lat_long_airports = {}

    def process(self, message):
        # message fields: AirportCode,Latitude,Longitude
        airport_code, latitude, longitude = (
            message["AirportCode"],
            message["Latitude"],
            message["Longitude"],
        )
        self.lat_long_airports[airport_code] = (latitude, longitude)

    def finish_processing(self, client_id):
        return EOF

    def get_lat_long_airports(self):
        return self.lat_long_airports
