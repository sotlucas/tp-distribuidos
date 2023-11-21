from commons.processor import Processor, Respose, ResponseType


class MaxAvg(Processor):
    def __init__(self, client_id):
        pass

    def process(self, message):
        # input message: route;prices
        # output message: route,avg,max_price

        route = message["route"]
        prices = message["prices"]
        prices = [float(price) for price in prices.split(";")]

        # 1. Calcula el avg y max de los precios.
        avg = self.get_avg(prices)
        max_price = self.get_max(prices)

        # 2. Formateo el resultado de salida.
        message = {"route": route, "avg": avg, "max_price": max_price}
        return Respose(ResponseType.SINGLE, message)

    def get_avg(self, prices):
        return sum(prices) / len(prices)

    def get_max(self, prices):
        return max(prices)

    def finish_processing(self):
        pass
