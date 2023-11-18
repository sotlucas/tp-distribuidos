import logging
from commons.processor import Processor
from commons.message import ProtocolMessage

STARTING_AIRPORT = "startingAirport"
DESTINATION_AIRPORT = "destinationAirport"
TOTAL_FARE = "totalFare"
AVERAGE = "average"

# TODO: remove this
TEMPORAL_CLIENT_ID = 0


class Grouper(Processor):
    """
    1. Agrupa totalFare por route.

    Cuando recibe el EOF:
    2. Suma todos los precios
    3. Envía el resultado junto con la cantidad al processor de media general.

    Send results to output:
    4. Filtra los precios que estén por encima de la media general.
    5. Finalmente, envía cada trayecto con los precios filtrados a la cola de salida.
    """

    def __init__(self, replica_id, media_general_receiver, media_general_sender):
        self.replica_id = replica_id
        self.media_general_receiver = media_general_receiver
        self.media_general_sender = media_general_sender

        self.media_general_input_fields = ["average"]
        self.media_general_output_fields = ["totalFare", "amount"]
        logging.info(f"Starting grouper {self.replica_id}")
        self.routes = {}
        self.waiting_for_media_general = False
        self.vuelos_message_to_send = []

    def process(self, message):
        # 1. Agrupa totalFare por route.
        self.group_prices_by_route(message)

    def group_prices_by_route(self, message):
        # message = startingAirport,destinationAirport,totalFare
        route = self.get_route(message)
        total_fare = self.get_total_fare(message)
        if route in self.routes:
            self.routes[route].append(total_fare)
        else:
            self.routes[route] = [total_fare]

    def get_route(self, message):
        return "{}-{}".format(
            message[STARTING_AIRPORT],
            message[DESTINATION_AIRPORT],
        )

    def get_total_fare(self, message):
        return float(message[TOTAL_FARE])

    def finish_processing(self):
        # 2. Suma todos los precios
        # 3. Envía el resultado junto con la cantidad al procesador de media general.
        self.media_general_receiver.bind(
            self.process_media_general,
            self.media_general_receiver.stop,
            self.media_general_sender,
            input_fields_order=self.media_general_input_fields,
        )
        total_fare = 0
        amount = 0
        for prices in self.routes.values():
            total_fare += sum(prices)
            amount += len(prices)
        message = {"totalFare": total_fare, "amount": amount}
        message_to_send = ProtocolMessage(TEMPORAL_CLIENT_ID, [message])
        self.media_general_sender.send_all(
            message_to_send,
            output_fields_order=self.media_general_output_fields,
        )
        self.waiting_for_media_general = True
        self.media_general_receiver.start()
        return self.vuelos_message_to_send

    def process_media_general(self, messages):
        for message in messages.payload:
            results = self.process_single(message)
            self.vuelos_message_to_send.extend(results)
        self.routes.clear()
        self.waiting_for_media_general = False
        # Stop media general receiver because it's not needed anymore & it doesn't send EOF
        self.media_general_receiver.stop()

    def process_single(self, message):
        # 4. Filtra los precios que estén por encima de la media general.
        # 5. Finalmente, envía cada trayecto con los precios filtrados a la cola de salida.
        media_general = float(message[AVERAGE])
        result = []
        for route, prices in self.routes.items():
            prices_filtered = self.filter_prices(prices, media_general)
            if prices_filtered:
                message = {
                    "route": route,
                    "prices": ";".join(map(str, prices_filtered)),
                }
                result.append(message)
        return result

    def filter_prices(self, prices, media_general):
        return list(filter(lambda price: price > media_general, prices))
