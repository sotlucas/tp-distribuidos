from commons.communication import Communication
import logging


class LatLongUploaderConfig:
    def __init__(self, file_path, remove_file_header):
        self.file_path = file_path
        self.remove_file_header = remove_file_header


class LatLongUploader:
    def __init__(self, config, communication_config):
        self.communication = Communication(communication_config)
        self.config = config

    def run(self):
        with open(self.config.file_path) as f:
            if self.config.remove_file_header:
                # Skip the header
                next(f)

            logging.info("Sending file")
            for line in f:
                line = line.rstrip()
                self.communication.send_output(line)
            logging.info("Finished sending file")
        self.communication.close()
