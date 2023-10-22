import logging

from commons.protocol import END_OF_MESSAGE


class FileUploader:
    def __init__(self, type, file_path, remove_file_header, batch_size, sock):
        self.type = type
        self.file_path = file_path
        self.remove_file_header = remove_file_header
        self.batch_size = batch_size
        self.sock = sock

    def start(self):
        """
        Send the csv file line by line to the server.

        Each line represents a flight with all the columns separated by commas.
        """
        # TODO: move this to the protocol serializer
        # Add the type of message to the beginning of the line
        if self.type == "airport":
            type_bytes = int.to_bytes(1, 1, byteorder="big")
        elif self.type == "flight":
            type_bytes = int.to_bytes(2, 1, byteorder="big")

        logging.info(f"Sending file: {self.file_path}")
        for batch in self.__next_batch(self.file_path, self.batch_size):
            message = type_bytes + batch.encode() + END_OF_MESSAGE
            self.sock.sendall(message)
        # Send message to indicate that the file has ended
        message = type_bytes + b"\0" + END_OF_MESSAGE
        self.sock.sendall(message)
        logging.info(f"File sent: {self.file_path}")

    def __next_batch(self, file_path, batch_size):
        """
        Gets a batch of rows from the file.
        """
        batch = []
        with open(file_path, "r") as f:
            if self.remove_file_header:
                # Skip the header
                next(f)
            for line in f:
                batch.append(line)
                if len(batch) == batch_size or not line:
                    yield "".join(batch)
                    batch = []
