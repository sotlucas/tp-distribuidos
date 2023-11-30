import json
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
                f.write(f"{json.dumps(message)}\n")
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

        Returns:
            A tuple with the restore type, message_id, client_id and state.
            restore_type: The type of the restore. It can be "COMMIT", "SAVE DONE" or "SENT".
            message_id: The id of the message.
            client_id: The id of the client.
            state: The state to restore.
        """
        with self.lock:
            try:
                lines = read_file_bottom_to_top_generator(self.log_file_path)
                line = next(lines)
                if line.startswith("COMMIT"):
                    return self.handle_commit(line, lines)
                elif line.startswith("SAVE DONE"):
                    return self.handle_save_done(line, lines)
                elif line.startswith("SAVE BEGIN") or line.startswith("SENT") or line.startswith("START"):
                    return self.handle_sent(line, lines)
            except (StopIteration, FileNotFoundError):
                # We reached the beggining of the file or the file doesn't exist
                return None, None, None, None

    def handle_commit(self, line, lines):
        # Go to the START of this message
        message_lines = []
        while not line.startswith("START"):
            message_lines.append(line)
            line = next(lines)
        # TODO: restore the state of the processor
        state = message_lines[-3]
        print(f"Restoring state: {state}")
        message_id, client_id = line.split("START")[1].split(" / ")
        return "COMMIT", int(message_id.strip()), int(client_id.strip()), json.loads(state)

    def handle_save_done(self, line, lines):
        # Go to the START of this message
        message_lines = []
        while not line.startswith("START"):
            message_lines.append(line)
            line = next(lines)
        # TODO: restore the state of the processor
        state = message_lines[-3]
        print(f"Restoring state: {state}")
        message_id, client_id = line.split("START")[1].split(" / ")
        # TODO: append message_id to the list of possible_duplicates
        print(f"Appending to possible duplicates: {message_id.strip()}")
        return "SAVE DONE", int(message_id.strip()), int(client_id.strip()), json.loads(state)

    def handle_sent(self, line, lines):
        # Go to the START of this message
        while not line.startswith("START"):
            line = next(lines)
        message_id, client_id = line.split("START")[1].split(" / ")
        # TODO: append message_id to the list of possible_duplicates
        print(f"Appending to possible duplicates: {message_id.strip()}")
        state = None
        try:
            line = next(lines)
            message_lines = []
            while not line.startswith("START"):
                message_lines.append(line)
                line = next(lines)
            # TODO: restore the state of the processor with the message before the last one
            if len(message_lines) > 3:
                state = message_lines[-3]
        except StopIteration:
            # We reached the beggining of the file
            pass
        print(f"Restoring state: {state}")
        if state:
            state = json.loads(state)
        return "SENT", int(message_id.strip()), int(client_id.strip()), state


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
