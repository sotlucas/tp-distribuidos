import threading


class State:
    def __init__(self):
        self.airports_by_client = {}
        self.state_by_client = {}
        self.lock = threading.Lock()

    def add_airport(self, client_id, airport_code, latitude, longitude):
        self.lock.acquire()
        if client_id not in self.state_by_client:
            self.state_by_client[client_id] = False

        airports = self.airports_by_client.get(client_id, {})
        airports[airport_code] = (latitude, longitude)
        self.airports_by_client[client_id] = airports
        self.lock.release()

    def all_airports_received(self, client_id):
        self.lock.acquire()
        self.state_by_client[client_id] = True
        self.lock.release()

    def is_client_done(self, client_id):
        self.lock.acquire()
        if client_id not in self.state_by_client:
            self.lock.release()
            return False
        state = self.state_by_client[client_id]
        self.lock.release()
        return state

    def get_airports(self, client_id):
        self.lock.acquire()
        if not self.state_by_client[client_id]:
            raise Exception(
                "Client {} not done receiving all airports".format(client_id)
            )
        airports = self.airports_by_client[client_id]
        self.lock.release()
        return airports

    def remove_client(self, client_id):
        self.lock.acquire()
        del self.airports_by_client[client_id]
        del self.state_by_client[client_id]
        self.lock.release()
