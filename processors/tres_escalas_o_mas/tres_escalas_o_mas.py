from commons.processor import Processor, Response, ResponseType

SEGMENTS_ARRIVAL_AIRPORT = "segmentsArrivalAirportCode"


class TresEscalasOMas(Processor):
    def __init__(self, client_id):
        pass

    def process(self, message):
        segmentsArrivalAirportCode = message[SEGMENTS_ARRIVAL_AIRPORT]
        arrivals = segmentsArrivalAirportCode.split("||")
        stopover = len(arrivals) - 1  # -1 because the last arrival is the destination
        if stopover >= 3:
            return Response(ResponseType.SINGLE, message)
        else:
            return None

    def finish_processing(self):
        pass
