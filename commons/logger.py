import multiprocessing as mp
import os


class Logger:
    """
    Logger used for durability and recovery.
    """

    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.lock = mp.Lock()

    def start(self, message_id, client_id):
        """
        Starts a message in the log file.
        With the format:
        START <message_id> / <client_id>
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"START {message_id} / {client_id}\n")

    def sent(self, message_id, client_id):
        """
        Sent a message in the log file.
        With the format:
        SENT <message_id> / <client_id>
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"SENT {message_id} / {client_id}\n")

    def save(self, message_id, client_id, message):
        """
        Saves a message in the log file.
        With the format:
        SAVE <Pickle or json of save information>
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"SAVE BEGIN {message_id} / {client_id}\n")
                f.write(f"{message}\n")
                f.write(f"SAVE DONE {message_id} / {client_id}\n")

    def commit(self, message_id, client_id):
        """
        Commits a message in the log file.
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"COMMIT {message_id} / {client_id}\n")

    def restore(self):
        """
        Restores the state of the processor from the log file.
        Starts reading the log file from the end to the beginning.
        """
        with self.lock:
            lines = read_file_bottom_to_top_generator(self.log_file_path)
            for line in lines:
                if line.startswith("COMMIT"):
                    # Go to the START of this message
                    while not line.startswith("START"):
                        line = next(lines)
                    # TODO: restore the state of the processor
                    print("Restoring state")
                elif line.startswith("SAVE DONE"):
                    # Go to the START of this message
                    while not line.startswith("START"):
                        line = next(lines)
                    # TODO: restore the state of the processor
                    print("Restoring state")
                    # TODO: append meesage_id to the list of possible_duplicates
                    print("Appending to possible duplicates")
                elif line.startswith("SAVE BEGIN") or line.startswith("SENT") or line.startswith("START"):
                    # Go to the START of this message
                    while not line.startswith("START"):
                        line = next(lines)
                    # TODO: append meesage_id to the list of possible_duplicates
                    print("Appending to possible duplicates")
                    while not line.startswith("START"):
                        line = next(lines)
                    # TODO: restore the state of the processor
                    print("Restoring state")
                    pass


def read_file_bottom_to_top_generator(filename, chunk_size=1024):
    """
    Generator that reads a file from the end to the beginning.
    """
    with open(filename, 'rb') as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        remainder = b''

        while file_size > 0:
            read_size = min(chunk_size, file_size)
            f.seek(-read_size, os.SEEK_CUR)
            chunk = f.read(read_size)
            f.seek(-read_size, os.SEEK_CUR)
            file_size -= read_size

            lines = (chunk + remainder).decode('utf-8').splitlines()

            # Save the last line if it's incomplete
            remainder = lines[0].encode('utf-8')
            lines.pop(0)

            for line in reversed(lines):
                yield line

        # Yield the last line if it's incomplete
        if remainder:
            yield remainder.decode('utf-8')
