import logging

STARTING_AIRPORT = "startingAirport"
DESTINATION_AIRPORT = "destinationAirport"
TOTAL_FARE = "totalFare"
AVERAGE = "average"


class Grouper:
    """
    1. Agrupa totalFare por route.

    Cuando recibe el EOF:
    2. Suma todos los precios
    3. Envía el resultado junto con la cantidad al processor de media general.

    Send results to output:
    4. Filtra los precios que estén por encima de la media general.
    5. Finalmente, envía cada trayecto con los precios filtrados a la cola de salida.
    """

    def __init__(
        self,
        replica_id,
        vuelos_receiver,
        vuelos_sender,
        media_general_receiver,
        media_general_sender,
    ):
        self.replica_id = replica_id
        self.vuelos_receiver = vuelos_receiver
        self.vuelos_sender = vuelos_sender
        self.media_general_receiver = media_general_receiver
        self.media_general_sender = media_general_sender

        self.vuelos_input_fields = [
            "startingAirport",
            "destinationAirport",
            "totalFare",
        ]
        self.vuelos_output_fields = ["route", "prices"]
        self.media_general_input_fields = ["average"]
        self.media_general_output_fields = ["totalFare", "amount"]
        logging.info(f"Starting grouper {self.replica_id}")
        self.routes = {}

    def run(self):
        self.vuelos_receiver.bind(
            self.process,
            self.send_results,
            self.vuelos_sender,
            input_fields_order=self.vuelos_input_fields,
        )
        self.vuelos_receiver.start()

    def process(self, messages):
        for message in messages:
            self.process_single(message)

    def process_single(self, message):
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

    def send_results(self):
        # 2. Suma todos los precios
        # 3. Envía el resultado junto con la cantidad al procesador de media general.
        self.media_general_receiver.bind(
            self.send_results_to_output,
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
        self.media_general_sender.send_all(
            [message],
            output_fields_order=self.media_general_output_fields,
        )
        self.media_general_receiver.start()

    def send_results_to_output(self, messages):
        for message in messages:
            results = self.send_results_to_output_single(message)
            self.vuelos_sender.send_all(
                results, output_fields_order=self.vuelos_output_fields
            )
        self.routes.clear()
        # self.vuelos_sender.send_eof()

    def send_results_to_output_single(self, message):
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
