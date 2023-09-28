import signal
import socket
import logging


class Client:
    def __init__(self, server_ip, server_port, file_path):
        self.server_ip = server_ip
        self.server_port = server_port
        self.file_path = file_path
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.shutdown)

    def run(self):
        # Create a socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to the server
        self.sock.connect((self.server_ip, self.server_port))
        logging.info("Connected to server")

        self.send_file()

    def send_file(self):
        """
        Send the csv file line by line to the server.

        Each line represents a flight with all the columns separated by commas.
        """

        logging.info("Sending file")
        with open(self.file_path, "r") as f:
            for line in f:
                try:
                    self.sock.send(line.encode())
                except OSError:
                    # When receiving SIGTERM, the socket is closed and a OSError is raised.
                    return
        logging.info("File sent")
        self.shutdown()

    def shutdown(self):
        logging.info("Shutting down")
        self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
        logging.info("Shut down completed")
