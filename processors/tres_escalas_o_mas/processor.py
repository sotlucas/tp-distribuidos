SEGMENTS_ARRIVAL_AIRPORT_CODE_INDEX = 5


class Processor:
    def proccess(self, message):
        params = message.split(",")
        segmentsArrivalAirportCode = params[SEGMENTS_ARRIVAL_AIRPORT_CODE_INDEX]
        arrivals = segmentsArrivalAirportCode.split("||")
        stopover = len(arrivals) - 1  # -1 because the last arrival is the destination
        if stopover >= 3:
            return message
        else:
            return None
