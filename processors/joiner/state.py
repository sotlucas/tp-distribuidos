import threading


class State:
    def __init__(self):
        self.airports_by_client = {}
        self.lock = threading.Lock()

    def add_airport(self, client_id, airport_code, latitude, longitude):
        self.lock.acquire()

        client_airports = self.airports_by_client.get(client_id, {})
        client_airports[airport_code] = (latitude, longitude)

        self.airports_by_client[client_id] = client_airports

        self.lock.release()

    def obtain_client_airport(self, client_id, airport_code):
        """
        Returns the airport with the given code for the given client.
        If the airport is not found, returns None.
        """
        self.lock.acquire()
        client_airports = self.airports_by_client.get(client_id, {})
        airport = client_airports.get(airport_code, None)
        self.lock.release()
        return airport

    def remove_client(self, client_id):
        self.lock.acquire()
        del self.airports_by_client[client_id]
        del self.state_by_client[client_id]
        self.lock.release()
