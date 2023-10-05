from processor import Processor


def processor_dos_mas_rapidos():
    vuelos = [
        "9ca,ATL,BOS,PT1H20M,BOS",
        "0e8,PHL,DFW,PT2H20M,DFW",
        "gf4,DFW,ATL,PT0H30M,ATL",
        "c68,LAX,BOS,PT1H30M,BOS"
    ]
    processor = Processor()
    for vuelo in vuelos:
        processor.proccess(vuelo)

    assert 2 == len(processor.fastest)
    assert "gf4,DFW,ATL,PT0H30M,ATL" == processor.fastest[0]
    assert "9ca,ATL,BOS,PT1H20M,BOS" == processor.fastest[1]


if __name__ == '__main__':
    processor_dos_mas_rapidos()
