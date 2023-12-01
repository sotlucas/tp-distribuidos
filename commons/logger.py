import json
import logging
import multiprocessing as mp
import os
from enum import Enum


class RestoreType(Enum):
    COMMIT = 0
    SAVE_DONE = 1
    SENT = 2


class LoggerToken:
    START = "START"
    SENT = "SENT"
    SAVE_BEGIN = "SAVE BEGIN"
    SAVE_DONE = "SAVE DONE"
    COMMIT = "COMMIT"


class Logger:
    """
    Logger used for durability and recovery.
    """

    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.lock = mp.Lock()

    def start(self, message_id, client_id):
        """
        Logs the start of a message in the log file.
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"{LoggerToken.START} {message_id} / {client_id}\n")

    def sent(self, message_id, client_id):
        """
        Logs a message as sent in the log file.
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"{LoggerToken.SENT} {message_id} / {client_id}\n")

    def save(self, message_id, client_id, message):
        """
        Saves a message in the log file.
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"{LoggerToken.SAVE_BEGIN} {message_id} / {client_id}\n")
                f.write(f"{json.dumps(message)}\n")
                f.write(f"{LoggerToken.SAVE_DONE} {message_id} / {client_id}\n")

    def commit(self, message_id, client_id):
        """
        Logs a message as committed in the log file.
        """
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(f"{LoggerToken.COMMIT} {message_id} / {client_id}\n")

    def restore(self):
        """
        Restores the state of the processor from the log file.

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
                if line.startswith(LoggerToken.COMMIT):
                    logging.debug("Restoring from COMMIT")
                    return self.__handle_commit(line, lines)
                elif line.startswith(LoggerToken.SAVE_DONE):
                    logging.debug("Restoring from SAVE DONE")
                    return self.__handle_save_done(line, lines)
                elif line.startswith(LoggerToken.SAVE_BEGIN) or line.startswith(LoggerToken.SENT) or line.startswith(
                        LoggerToken.START):
                    logging.debug("Restoring from SENT")
                    return self.__handle_sent(line, lines)
            except (StopIteration, FileNotFoundError):
                # We reached the beggining of the file or the file doesn't exist
                pass
            return None, None, None, None

    def __handle_commit(self, line, lines):
        return self.__get_last_message(RestoreType.COMMIT, line, lines)

    def __handle_save_done(self, line, lines):
        return self.__get_last_message(RestoreType.SAVE_DONE, line, lines)

    def __handle_sent(self, line, lines):
        # Go to the START of this message
        while not line.startswith(LoggerToken.START):
            line = next(lines)
        message_id, client_id = line.split(LoggerToken.START)[1].split(" / ")
        state = None
        try:
            # Restore from the last COMMITed message
            line = next(lines)
            while not line.startswith(LoggerToken.COMMIT):
                line = next(lines)
            # Go to the START of this message
            message_lines = []
            while not line.startswith(LoggerToken.START):
                message_lines.append(line)
                line = next(lines)
            state = message_lines[-3]
        except StopIteration:
            # We reached the beggining of the file
            pass
        if state:
            state = json.loads(state)
        return RestoreType.SENT, int(message_id.strip()), int(client_id.strip()), state

    def __get_last_message(self, restore_type, line, lines):
        # Go to the START of this message
        message_lines = []
        while not line.startswith(LoggerToken.START):
            message_lines.append(line)
            line = next(lines)
        state = message_lines[-3]
        message_id, client_id = line.split(LoggerToken.START)[1].split(" / ")
        return restore_type, int(message_id.strip()), int(client_id.strip()), json.loads(state)


def read_file_bottom_to_top_generator(filename, chunk_size=1024):
    """
    Generator that reads a file from the end to the beginning.

    Parameters:
        filename: The path of the file to read.
        chunk_size: The size of the chunks to read.
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
