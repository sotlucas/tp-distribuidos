class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender

    def run(self):
        self.receiver.bind(
            input_callback=self.process, eof_callback=self.sender.send_eof
        )
        self.receiver.start()

    def process(self, messages):
        processed_messages = []
        for message in messages:
            processed_message = self.process_single(message)
            if processed_message:
                processed_messages.append(processed_message)
        if len(processed_messages) > 0:
            self.sender.send_all(processed_messages)

    def process_single(self, message):
        # input message: route;prices
        # output message: route,avg,max_price

        route, prices = message.split(";")
        prices = [float(price) for price in prices.split(",")]

        # 1. Calcula el avg y max de los precios.
        avg = self.get_avg(prices)
        max_price = self.get_max(prices)

        # 2. Formateo el resultado de salida.
        return "{},{},{}".format(route, avg, max_price)

    def get_avg(self, prices):
        return sum(prices) / len(prices)

    def get_max(self, prices):
        return max(prices)
