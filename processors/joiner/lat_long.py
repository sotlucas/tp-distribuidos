from commons.processor import Processor


class LatLongConfig:
    def __init__(self, state):
        self.state = state


class LatLong(Processor):
    def __init__(self, config, client_id):
        self.config = config
        self.client_id = client_id

    def process(self, message):
        # message fields: AirportCode,Latitude,Longitude
        airport_code, latitude, longitude = (
            message["AirportCode"],
            message["Latitude"],
            message["Longitude"],
        )
        self.config.state.add_airport(self.client_id, airport_code, latitude, longitude)

    def finish_processing(self):
        self.config.state.all_airports_received(self.client_id)
