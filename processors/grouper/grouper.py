import logging
from commons.processor import Processor, Response, ResponseType
from commons.message import ProtocolMessage

STARTING_AIRPORT = "startingAirport"
DESTINATION_AIRPORT = "destinationAirport"
TOTAL_FARE = "totalFare"
AVERAGE = "average"


class GrouperConfig:
    def __init__(
        self,
        replica_id,
        media_general_log_guardian,
        media_general_communication_initializer,
        media_general_input,
        input_type,
        replicas_count,
        input_diff_name,
        media_general_output,
        output_type,
    ):
        self.replica_id = replica_id
        self.media_general_log_guardian = media_general_log_guardian
        self.media_general_communication_initializer = (
            media_general_communication_initializer
        )
        self.media_general_input = media_general_input
        self.input_type = input_type
        self.replicas_count = replicas_count
        self.input_diff_name = input_diff_name
        self.media_general_output = media_general_output
        self.output_type = output_type


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

    def __init__(self, config, client_id):
        self.replica_id = config.replica_id
        self.client_id = client_id
        self.media_general_log_guardian = config.media_general_log_guardian
        self.media_general_receiver = config.media_general_communication_initializer.initialize_receiver(
            config.media_general_input,
            config.input_type,
            config.replica_id,
            config.replicas_count,
            # TODO: This is to differentiate the queues between clients, see if this is the best way to do it
            input_diff_name=config.input_diff_name + "_" + str(client_id),
        )
        self.media_general_sender = (
            config.media_general_communication_initializer.initialize_sender(
                config.media_general_output, config.output_type
            )
        )

        self.media_general_input_fields = ["average"]
        self.media_general_output_fields = ["totalFare", "amount"]
        logging.info(f"Starting grouper {self.replica_id}")

        self.routes = {}
        self.vuelos_message_to_send = []
        self.waiting_for_media_general = False

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
        self.restore_media_general()

        if self.vuelos_message_to_send:
            # It means we already processed the EOF but we didn't send the results
            return Response(ResponseType.MULTIPLE, self.vuelos_message_to_send)

        # 2. Suma todos los precios
        # 3. Envía el resultado junto con la cantidad al procesador de media general.
        self.media_general_receiver.bind(
            input_callback=self.process_media_general,
            eof_callback=self.media_general_receiver.stop,
            sender=self.media_general_sender,
            input_fields_order=self.media_general_input_fields,
        )
        total_fare = 0
        amount = 0
        for prices in self.routes.values():
            total_fare += sum(prices)
            amount += len(prices)
        message = {"totalFare": total_fare, "amount": amount}

        # TODO: The message_id is the replica_id, to differentiate the messages
        #       sent by other replicas, see if this is the best way to do it.
        message_to_send = ProtocolMessage(self.client_id, self.replica_id, [message])
        self.media_general_sender.send_all(
            message_to_send,
            output_fields_order=self.media_general_output_fields,
        )
        self.waiting_for_media_general = True
        self.media_general_receiver.start()

        return Response(ResponseType.MULTIPLE, self.vuelos_message_to_send)

    def restore_media_general(self):
        clients_ids = self.media_general_log_guardian.obtain_all_active_clients()
        if self.client_id in clients_ids:
            all_messages = (
                self.media_general_log_guardian.search_for_all_connection_messages(
                    self.client_id
                )
            )
            for message_batch in all_messages:
                for message in message_batch:
                    self.process_single(message)

    def process_media_general(self, messages):
        # We save the message to disk to be able to recover it
        self.media_general_log_guardian.store_new_connection_message(messages.payload)

        for message in messages.payload:
            self.process_single(message)

        # Stop media general receiver because it's not needed anymore & it doesn't send EOF
        self.media_general_receiver.stop()
        self.waiting_for_media_general = False

    def process_single(self, message):
        # 4. Filtra los precios que estén por encima de la media general.
        # 5. Finalmente, envía cada trayecto con los precios filtrados a la cola de salida.
        media_general = float(message[AVERAGE])
        logging.debug(f"Media general received: {media_general}")
        result = []
        for route, prices in self.routes.items():
            prices_filtered = self.filter_prices(prices, media_general)
            if prices_filtered:
                message = {
                    "route": route,
                    "prices": ";".join(map(str, prices_filtered)),
                }
                result.append(message)
        self.vuelos_message_to_send.extend(result)

    def filter_prices(self, prices, media_general):
        return list(filter(lambda price: price > media_general, prices))
