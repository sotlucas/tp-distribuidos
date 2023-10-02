import signal
import socket
import logging


class ClientConfig:
    def __init__(self, server_ip, server_port, file_path, remove_file_header) -> None:
        self.server_ip = server_ip
        self.server_port = server_port
        self.file_path = file_path
        self.remove_file_header = remove_file_header


class Client:
    def __init__(self, config):
        self.config = config
        self.running = True
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.shutdown)

    def run(self):
        # Create a socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to the server
        self.sock.connect((self.config.server_ip, self.config.server_port))
        logging.info("Connected to server")

        self.send_file()

    def send_file(self):
        """
        Send the csv file line by line to the server.

        Each line represents a flight with all the columns separated by commas.
        """

        logging.info("Sending file")
        with open(self.config.file_path, "r") as f:
            if self.config.remove_file_header:
                # Skip the header
                next(f)

            for line in f:
                try:
                    bytes = line.rstrip().encode()
                    # Add the \r\n\r\n sequence to mark the end of the message
                    bytes += b"\r\n\r\n"
                    bytes_to_send = len(bytes)

                    while bytes_to_send > 0:
                        # Send the data
                        sent = self.sock.send(bytes)
                        bytes_to_send -= sent
                        bytes = bytes[sent:]

                except OSError as e:
                    # When receiving SIGTERM, the socket is closed and a OSError is raised.
                    # If not we want to raise the exception.
                    if self.running:
                        raise e
                    return

        logging.info("File sent")
        self.shutdown()

    def shutdown(self):
        logging.info("Shutting down")
        self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
        self.running = False
        logging.info("Shut down completed")
