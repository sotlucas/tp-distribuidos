import logging

STARTING_AIRPORT_INDEX = 0
DESTINATION_AIRPORT_INDEX = 1
TOTAL_FARE_INDEX = 2


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
        logging.info(f"Starting grouper {self.replica_id}")
        self.routes = {}

    def run(self):
        self.vuelos_receiver.bind(self.process, self.send_results)
        self.vuelos_receiver.start()

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
        message_split = message.split(",")
        return "{}-{}".format(
            message_split[STARTING_AIRPORT_INDEX],
            message_split[DESTINATION_AIRPORT_INDEX],
        )

    def get_total_fare(self, message):
        message_split = message.split(",")
        return float(message_split[TOTAL_FARE_INDEX])

    def send_results(self):
        # 2. Suma todos los precios
        # 3. Envía el resultado junto con la cantidad al procesador de media general.
        self.media_general_receiver.bind(
            self.send_results_to_output, self.media_general_receiver.stop
        )
        total_fare = 0
        amount = 0
        for prices in self.routes.values():
            total_fare += sum(prices)
            amount += len(prices)

        self.media_general_sender.send("{},{}".format(total_fare, amount))
        self.media_general_receiver.start()

    def send_results_to_output(self, message):
        # 4. Filtra los precios que estén por encima de la media general.
        # 5. Finalmente, envía cada trayecto con los precios filtrados a la cola de salida.
        media_general = float(message)
        for route, prices in self.routes.items():
            prices_filtered = self.filter_prices(prices, media_general)
            if prices_filtered:
                self.vuelos_sender.send(
                    "{};{}".format(route, ",".join(map(str, prices_filtered)))
                )
        self.vuelos_sender.send_eof()

    def filter_prices(self, prices, media_general):
        return list(filter(lambda price: price > media_general, prices))
