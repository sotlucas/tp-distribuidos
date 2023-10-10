class Processor:
    def __init__(self, communication):
        self.communication = communication

    def run(self):
        self.communication.run(input_callback=self.process)

    def process(self, message):
        # input message: route;prices
        # output message: route,avg,max_price

        route, prices = message.split(";")
        prices = [float(price) for price in prices.split(",")]

        # 1. Calcula el avg y max de los precios.
        avg = self.get_avg(prices)
        max_price = self.get_max(prices)

        # 2. Envía el resultado al procesador de salida.
        self.communication.send_output("{},{},{}".format(route, avg, max_price))

    def get_avg(self, prices):
        return sum(prices) / len(prices)

    def get_max(self, prices):
        return max(prices)
