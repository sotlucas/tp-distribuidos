from commons.processor import Processor


class MaxAvg(Processor):
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
        return message

    def get_avg(self, prices):
        return sum(prices) / len(prices)

    def get_max(self, prices):
        return max(prices)

    def finish_processing(self, client_id):
        pass
