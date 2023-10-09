import datetime


class ResultHandler:
    def __init__(self):
        self.tstamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def save_results(self, data):
        """
        Saves the results in the corresponding file.
        """
        # get the tag between [] to identify the file
        file_name = data.split("[")[1].split("]")[0].lower()

        with open(f"results/{self.tstamp}_{file_name}.txt", "a") as f:
            f.write(data + "\n")
