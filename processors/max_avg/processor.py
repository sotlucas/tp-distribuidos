class Processor:
    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender

        self.input_fields = ["route", "prices"]
        self.output_fields = ["route", "avg", "max_price"]

    def run(self):
        self.receiver.bind(
            input_callback=self.process,
            eof_callback=self.sender.send_eof,
            sender=self.sender,
            input_fields_order=self.input_fields,
        )
        self.receiver.start()

    def process(self, messages):
        processed_messages = []
        for message in messages:
            processed_message = self.process_single(message)
            if processed_message:
                processed_messages.append(processed_message)
        self.sender.send_all(processed_messages, output_fields_order=self.output_fields)

    def process_single(self, message):
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
