from processor import Processor


def test_multiple_flights_for_same_trajectory():
    vuelos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "okl,ATL,BOS,PT8H43M,BOS",
        "asd,ATL,BOS,PT2H12M,BOS",
    ]
    processor = Processor(MockedCommunication())
    for vuelo in vuelos:
        processor.proccess(vuelo)

    assert 1 == len(processor.trajectory)
    assert ["9ca,ATL,BOS,PT1H20M,BOS", "asd,ATL,BOS,PT2H12M,BOS"] == processor.trajectory['ATL-BOS']


def test_multiple_flights_multiple_trajectories():
    vuelos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "okl,ATL,BOS,PT8H43M,BOS",
        "asd,ATL,BOS,PT2H12M,BOS",
        "0e8,PHL,DFW,PT2H20M,DFW",
        "c68,DFW,ATL,PT1H30M,ATL",
        "a67,DFW,ATL,PT2H30M,ATL",
        "gf4,DFW,ATL,PT0H30M,ATL",
    ]
    processor = Processor(MockedCommunication())
    for vuelo in vuelos:
        processor.proccess(vuelo)

    assert 3 == len(processor.trajectory)
    assert ["9ca,ATL,BOS,PT1H20M,BOS", "asd,ATL,BOS,PT2H12M,BOS"] == processor.trajectory['ATL-BOS']
    assert ["0e8,PHL,DFW,PT2H20M,DFW"] == processor.trajectory['PHL-DFW']
    assert ["gf4,DFW,ATL,PT0H30M,ATL", "c68,DFW,ATL,PT1H30M,ATL"] == processor.trajectory['DFW-ATL']


def test_multiple_flights_single_trajectories():
    vuelos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "0e8,PHL,DFW,PT2H20M,DFW",
        "gf4,DFW,ATL,PT0H30M,ATL",
        "c68,LAX,BOS,PT1H30M,BOS",
    ]
    processor = Processor(MockedCommunication())
    for vuelo in vuelos:
        processor.proccess(vuelo)

    assert 4 == len(processor.trajectory)
    assert ["9ca,ATL,BOS,PT1H20M,BOS"] == processor.trajectory['ATL-BOS']
    assert ["0e8,PHL,DFW,PT2H20M,DFW"] == processor.trajectory['PHL-DFW']
    assert ["gf4,DFW,ATL,PT0H30M,ATL"] == processor.trajectory['DFW-ATL']
    assert ["c68,LAX,BOS,PT1H30M,BOS"] == processor.trajectory['LAX-BOS']


def test_duration_format():
    duration = "PT1H30M"
    processor = Processor(MockedCommunication())
    assert 90 == processor.convert_travel_duration(duration)


def test_duration_format_days():
    duration = "P1DT8M"
    processor = Processor(MockedCommunication())
    assert 1448 == processor.convert_travel_duration(duration)


def test_duration_format_only_day():
    duration = "P2D"
    processor = Processor(MockedCommunication())
    assert 2880 == processor.convert_travel_duration(duration)


class MockedCommunication:
    def run(self, input_callback, eof_callback):
        pass

    def send_output(self, message):
        pass

    def send_eof(self):
        pass
