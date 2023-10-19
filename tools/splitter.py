import os
import sys
import pandas as pd

"""
Script que recibe un archivo CSV y crea un archivo con las primeras N cantidad de filas
Ejemplo de uso:
    python tools/splitter.py itineraries.csv 100
"""


class FileSettings(object):
    def __init__(self, file_path, row_size=100):
        self.file_path = file_path
        self.file_name = file_path.split("/")[1].split(".")[0]
        self.row_size = row_size


class FileSplitter(object):
    def __init__(self, file_settings: FileSettings):
        self.file_settings = file_settings

        if type(self.file_settings).__name__ != "FileSettings":
            raise Exception("Please pass correct instance ")

        self.df = pd.read_csv(self.file_settings.file_path,
                              chunksize=self.file_settings.row_size)

    def run(self, directory="data"):
        try:
            os.makedirs(directory)
        except Exception:
            pass

        file_name = "{}/archivo_{}.csv".format(directory, self.file_settings.row_size)
        next(self.df).to_csv(file_name, index=False)


def main():
    file_path = sys.argv[1] if len(sys.argv) > 1 else "../itineraries.csv"
    row_size = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    print(f"Splitting {file_path} into a file with {row_size} rows")

    splitter = FileSplitter(FileSettings(
        file_path=file_path,
        row_size=row_size
    ))
    splitter.run()


main()
