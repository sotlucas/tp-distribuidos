from grouper import Grouper


def test_multiple_flights_multiple_routes():
    vuelos = [
        "ATL,BOS,248.6",
        "PHL,DFW,168.59",
        "ATL,BOS,531.2",
        "LAX,BOS,491.59",
        "DFW,ATL,118.4",
    ]
    grouper = Grouper(1, MockedCommunication(), MockedCommunication())
    for vuelo in vuelos:
        grouper.process(vuelo)

    assert 4 == len(grouper.routes)
    assert [248.6, 531.2] == grouper.routes["ATL-BOS"]
    assert [168.59] == grouper.routes["PHL-DFW"]
    assert [491.59] == grouper.routes["LAX-BOS"]
    assert [118.4] == grouper.routes["DFW-ATL"]


class MockedCommunication:
    def run(self, input_callback, eof_callback):
        pass

    def send_output(self, message):
        pass

    def send_eof(self):
        pass
