import csv
import sys
from enum import Enum


class ResultType(Enum):
    TRES_ESCALAS = "TRES_ESCALAS"
    DISTANCIAS = "DISTANCIAS"
    MAX_AVG = "MAX_AVG"
    DOS_MAS_RAPIDOS = "DOS_MAS_RAPIDOS"


class ResultChecker:
    def __init__(self, result_file, fixture_file, result_type):
        self.result_file = result_file
        self.fixture_file = fixture_file
        self.result_type = result_type

    def check(self):
        """
        Checks if the result file is correct. The result_file has to be sorted with sort.
        """
        if self.result_type == ResultType.TRES_ESCALAS:
            return self.__check_tres_escalas()
        elif self.result_type == ResultType.DISTANCIAS:
            return self.__check_distancias()
        elif self.result_type == ResultType.MAX_AVG:
            return self.__check_max_avg()
        elif self.result_type == ResultType.DOS_MAS_RAPIDOS:
            return self.__check_dos_mas_rapidos()
        else:
            raise Exception("Invalid result type")

    def __check_tres_escalas(self):
        return self.__simple_compare()

    def __check_distancias(self):
        return self.__simple_compare()

    def __check_max_avg(self):
        """
        Rounds the average and maximum to 2 decimals and compares each line with the fixture file.
        """
        with open(self.result_file, newline="") as result_csv, open(
            self.fixture_file, newline=""
        ) as fixture_csv:
            result_reader = csv.reader(result_csv, delimiter=",")
            fixture_reader = csv.reader(fixture_csv, delimiter=",")
            for result_row, fixture_row in zip(result_reader, fixture_reader):
                if result_row[0] != fixture_row[0]:
                    print(
                        f"Route name is different: {result_row[0]} != {fixture_row[0]}"
                    )
                    return False
                if round(float(result_row[1]), 2) != round(float(fixture_row[1]), 2):
                    print(f"Average is different: {result_row[1]} != {fixture_row[1]}")
                    return False
                if round(float(result_row[2]), 2) != round(float(fixture_row[2]), 2):
                    print(f"Maximum is different: {result_row[2]} != {fixture_row[2]}")
                    return False
            return True

    def __check_dos_mas_rapidos(self):
        """
        Compares the two fastest routes for each route name.
        """
        with open(self.result_file, newline="") as result_csv, open(
            self.fixture_file, newline=""
        ) as fixture_csv:
            result_reader = csv.reader(result_csv, delimiter=",")
            fixture_reader = csv.reader(fixture_csv, delimiter=",")
            for result_row, fixture_row in zip(result_reader, fixture_reader):
                if result_row[1] != fixture_row[1]:
                    print(f"Origin is different: {result_row[1]} != {fixture_row[1]}")
                    return False
                if result_row[2] != fixture_row[2]:
                    print(
                        f"Destination is different: {result_row[2]} != {fixture_row[2]}"
                    )
                    return False
                if result_row[3] != fixture_row[3]:
                    print(f"Duration is different: {result_row[3]} != {fixture_row[3]}")
                    return False
            return True

    def __simple_compare(self):
        """
        Compares each line of the result file with the fixture file.
        """
        with open(self.result_file, newline="") as result_csv, open(
            self.fixture_file, newline=""
        ) as fixture_csv:
            result_reader = csv.reader(result_csv, delimiter=",")
            fixture_reader = csv.reader(fixture_csv, delimiter=",")
            for result_row, fixture_row in zip(result_reader, fixture_reader):
                if result_row != fixture_row:
                    return False
            return True


def main(result_file, fixture_file, result_type):
    result_checker = ResultChecker(result_file, fixture_file, result_type)
    if result_checker.check():
        print(f"The file {result_file} is correct")
    else:
        print(f"The file {result_file} is incorrect")


if __name__ == "__main__":
    result_file = sys.argv[1]
    fixture_file = sys.argv[2]
    result_type = ResultType[sys.argv[3].upper()]
    main(result_file, fixture_file, result_type)
