import logging


class LatLongUploaderConfig:
    def __init__(self, file_path, remove_file_header):
        self.file_path = file_path
        self.remove_file_header = remove_file_header


class LatLongUploader:
    def __init__(self, config, sender):
        self.sender = sender
        self.config = config

    def run(self):
        with open(self.config.file_path) as f:
            if self.config.remove_file_header:
                # Skip the header
                next(f)

            logging.info("Sending file")
            for line in f:
                line = line.rstrip()
                self.sender.send(line)
            logging.info("Finished sending file")
            self.sender.send_eof()
        self.sender.close()
