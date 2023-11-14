# Test skipped, change the name of the file to processor_test.py to run the test.
# It is skipped because it fails importing the commons package.

from dos_mas_rapidos import DosMasRapidos


def test_multiple_flights_for_same_trajectory():
    vuelos_datos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "okl,ATL,BOS,PT8H43M,BOS",
        "asd,ATL,BOS,PT2H12M,BOS",
    ]
    vuelos = create_vuelos(vuelos_datos)

    processor = DosMasRapidos()
    processor.process(vuelos)

    assert 1 == len(processor.trajectory)
    assert [
        vuelos[0],
        vuelos[2],
    ] == processor.trajectory["ATL-BOS"]


def test_multiple_flights_multiple_trajectories():
    vuelos_datos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "okl,ATL,BOS,PT8H43M,BOS",
        "asd,ATL,BOS,PT2H12M,BOS",
        "0e8,PHL,DFW,PT2H20M,DFW",
        "c68,DFW,ATL,PT1H30M,ATL",
        "a67,DFW,ATL,PT2H30M,ATL",
        "gf4,DFW,ATL,PT0H30M,ATL",
    ]
    vuelos = create_vuelos(vuelos_datos)

    processor = DosMasRapidos()
    processor.process(vuelos)

    assert 3 == len(processor.trajectory)
    assert [
        vuelos[0],
        vuelos[2],
    ] == processor.trajectory["ATL-BOS"]
    assert [vuelos[3]] == processor.trajectory["PHL-DFW"]
    print(processor.trajectory["DFW-ATL"])
    assert [
        vuelos[6],
        vuelos[4],
    ] == processor.trajectory["DFW-ATL"]


def test_multiple_flights_single_trajectories():
    vuelos_datos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "0e8,PHL,DFW,PT2H20M,DFW",
        "gf4,DFW,ATL,PT0H30M,ATL",
        "c68,LAX,BOS,PT1H30M,BOS",
    ]
    vuelos = create_vuelos(vuelos_datos)

    processor = DosMasRapidos()
    processor.process(vuelos)

    assert 4 == len(processor.trajectory)
    assert [vuelos[0]] == processor.trajectory["ATL-BOS"]
    assert [vuelos[1]] == processor.trajectory["PHL-DFW"]
    assert [vuelos[2]] == processor.trajectory["DFW-ATL"]
    assert [vuelos[3]] == processor.trajectory["LAX-BOS"]


def test_duration_format():
    duration = "PT1H30M"
    processor = DosMasRapidos()
    assert 90 == processor.convert_travel_duration(duration)


def test_duration_format_days():
    duration = "P1DT8M"
    processor = DosMasRapidos()
    assert 1448 == processor.convert_travel_duration(duration)


def test_duration_format_only_day():
    duration = "P2D"
    processor = DosMasRapidos()
    assert 2880 == processor.convert_travel_duration(duration)


# Aux
def create_vuelos(vuelos_datos):
    vuelos = []
    for vuelo in vuelos_datos:
        vuelo = vuelo.split(",")
        vuelos.append(
            {
                "legId": vuelo[0],
                "startingAirport": vuelo[1],
                "destinationAirport": vuelo[2],
                "travelDuration": vuelo[3],
                "segmentsArrivalAirportCode": vuelo[4],
            }
        )
    return vuelos
