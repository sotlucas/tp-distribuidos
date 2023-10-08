import datetime


class ResultHandler():
    def __init__(self):
        self.tstamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def save_results(self, data):
        """
        Saves the results in the corresponding file.
        """
        if "TRES_ESCALAS" in data:
            file_name = "tres_escalas"
        elif "DOS_MAS_RAPIDOS" in data:
            file_name = "dos_mas_rapidos"
        else:
            file_name = "results_general"
        with open(f"results/{self.tstamp}_{file_name}.txt", "a") as f:
            f.write(data + "\n")
