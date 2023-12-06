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


CONNECTION_LOG_FILE_PATH = "connection_log.txt"
COMMUNICATION_LOG_FILE_PATH = "communication_log.txt"
DUPLICATE_CATCHER_LOG_FILE_PATH = "duplicate_catcher_log.txt"


class Logger:
    """
    Logger used for durability and recovery.
    """

    def __init__(self, suffix=""):
        self.suffix = suffix
        self.communication_log_file_path = f"{COMMUNICATION_LOG_FILE_PATH}{self.suffix}"
        self.lock = mp.Lock()

    def start(self, message_id, client_id):
        """
        Logs the start of a message in the log file.
        """
        with self.lock:
            with open(self.communication_log_file_path, "a") as f:
                f.write(f"{LoggerToken.START} {message_id} / {client_id}\n")

                # Flush the file to disk
                f.flush()
                os.fsync(f.fileno())

    def sent(self, message_id, client_id):
        """
        Logs a message as sent in the log file.
        """
        with self.lock:
            with open(self.communication_log_file_path, "a") as f:
                f.write(f"{LoggerToken.SENT} {message_id} / {client_id}\n")

                # Flush the file to disk
                f.flush()
                os.fsync(f.fileno())

    def save_communication(self, message_id, client_id, message):
        """
        Saves a message in the log file.
        """
        with self.lock:
            with open(self.communication_log_file_path, "a") as f:
                f.write(f"{LoggerToken.SAVE_BEGIN} {message_id} / {client_id}\n")
                f.write(f"{json.dumps(message)}\n")
                f.write(f"{LoggerToken.SAVE_DONE} {message_id} / {client_id}\n")

                # Flush the file to disk
                f.flush()
                os.fsync(f.fileno())

    def save_connection(self, message_id, client_id, messages):
        """
        Appends a message to the connection log file.
        """
        with self.lock:
            file_path = f"{client_id}_{CONNECTION_LOG_FILE_PATH}{self.suffix}"
            with open(file_path, "a") as f:
                f.write(f"{message_id}/{json.dumps(messages)}\n")

                # Flush the file to disk
                f.flush()
                os.fsync(f.fileno())

    def save_duplicate_catcher(self, message_id, client_id):
        """
        Saves a message to the duplicate catcher log file.
        """
        with self.lock:
            file_path = f"{client_id}_{DUPLICATE_CATCHER_LOG_FILE_PATH}{self.suffix}"
            with open(file_path, "a") as f:
                f.write(f"{message_id}\n")

                # Flush the file to disk
                f.flush()
                os.fsync(f.fileno())

    def commit(self, message_id, client_id):
        """
        Logs a message as committed in the log file.
        """
        with self.lock:
            with open(self.communication_log_file_path, "a") as f:
                f.write(f"{LoggerToken.COMMIT} {message_id} / {client_id}\n")

                # Flush the file to disk
                f.flush()
                os.fsync(f.fileno())

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
                lines = read_file_bottom_to_top_generator(
                    self.communication_log_file_path
                )
                line = next(lines)
                if line.startswith(LoggerToken.COMMIT):
                    logging.debug("Restoring from COMMIT")
                    return self.__handle_commit(line, lines)
                elif line.startswith(LoggerToken.SAVE_DONE):
                    logging.debug("Restoring from SAVE DONE")
                    return self.__handle_save_done(line, lines)
                elif (
                    line.startswith(LoggerToken.SAVE_BEGIN)
                    or line.startswith(LoggerToken.SENT)
                    or line.startswith(LoggerToken.START)
                ):
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
            # Go to the SAVE DONE of this message
            while not line.startswith(LoggerToken.SAVE_DONE):
                line = next(lines)
            state = next(lines)
        except StopIteration:
            # We reached the beggining of the file
            pass
        if state:
            state = json.loads(state)

        try:
            self.delete_connection_messages(message_id.strip(), client_id.strip())
        except (FileNotFoundError, StopIteration):
            logging.debug(
                "The connection log file doesn't exist or it is empty, nothing to delete"
            )
            pass

        try:
            self.delete_duplicate_catcher_messages(
                message_id.strip(), client_id.strip()
            )
        except (FileNotFoundError, StopIteration):
            logging.debug(
                "The duplicate catcher log file doesn't exist or it is empty, nothing to delete"
            )
            pass

        return RestoreType.SENT, int(message_id.strip()), int(client_id.strip()), state

    def __get_last_message(self, restore_type, line, lines):
        # Go to the START of this message
        while not line.startswith(LoggerToken.SAVE_DONE):
            line = next(lines)
        state = next(lines)
        while not line.startswith(LoggerToken.START):
            line = next(lines)
        message_id, client_id = line.split(LoggerToken.START)[1].split(" / ")
        return (
            restore_type,
            int(message_id.strip()),
            int(client_id.strip()),
            json.loads(state),
        )

    def delete_connection_messages(self, message_id, client_id):
        """
        Deletes if necessary the messages of a connection from the connection log file.
        """
        file_path = f"{client_id}_{CONNECTION_LOG_FILE_PATH}{self.suffix}"
        lines = read_file_bottom_to_top_generator(file_path)
        line_to_search = next(lines)
        if line_to_search.startswith(str(message_id)):
            logging.debug(
                f"Deleting last connection message {message_id} of client {client_id}"
            )
            truncate_last_line_of_file(file_path)

    def delete_duplicate_catcher_messages(self, message_id, client_id):
        """
        Deletes if necessary the messages of a connection from the duplicate catcher log file.
        """
        file_path = f"{client_id}_{DUPLICATE_CATCHER_LOG_FILE_PATH}{self.suffix}"
        lines = read_file_bottom_to_top_generator(file_path)
        line_to_search = next(lines)
        if line_to_search.startswith(str(message_id)):
            logging.debug(
                f"Deleting last duplicate catcher message {message_id} of client {client_id}"
            )
            truncate_last_line_of_file(file_path)

    def search_processed(self, client_id, ids_to_search):
        """
        Searches if the given ids were processed and sent.
        """
        processed_ids_found = []
        with self.lock:
            try:
                lines = read_file_bottom_to_top_generator(
                    self.communication_log_file_path
                )
                for line in lines:
                    if line.startswith(LoggerToken.SAVE_DONE):
                        message_id, message_client_id = line.split(
                            LoggerToken.SAVE_DONE
                        )[1].split(" / ")
                        if (
                            int(message_client_id.strip()) != client_id
                            or int(message_id.strip()) not in ids_to_search
                        ):
                            continue
                        try:
                            while not line.startswith(
                                LoggerToken.START
                            ) and not line.startswith(LoggerToken.SENT):
                                line = next(lines)
                        except StopIteration:
                            # We reached the beggining of the file
                            pass
                        if line.startswith(LoggerToken.SENT):
                            processed_ids_found.append(message_id.strip() + "S")
                        elif line.startswith(LoggerToken.START):
                            processed_ids_found.append(message_id.strip())
            except FileNotFoundError:
                # The file doesn't exist
                logging.debug("The file doesn't exist, nothing to search")
                return processed_ids_found
        return processed_ids_found

    def obtain_all_connection_messages(self, client_id):
        """
        Obtains all connection messages from the connection log file.
        """
        file_path = f"{client_id}_{CONNECTION_LOG_FILE_PATH}{self.suffix}"
        messages = []
        with self.lock:
            try:
                lines = read_file_bottom_to_top_generator(file_path)
                for line in lines:
                    message_id, message = line.split("/", 1)
                    messages.append(json.loads(message.strip()))
            except FileNotFoundError:
                # The file doesn't exist
                logging.debug("The file doesn't exist, no connection messages found")
                return messages
        return messages

    def obtain_all_active_connection_clients(self):
        """
        Obtains all the active clients from all the connections log files.

        It does this by reading the names of the files and extracting the client ids from them.
        """
        file_names = os.listdir()
        client_ids = []
        for file_name in file_names:
            if file_name.endswith(f"{CONNECTION_LOG_FILE_PATH}{self.suffix}"):
                client_id = file_name.split("_")[0]
                client_ids.append(int(client_id))
        logging.debug(f"Active connection clients: {client_ids}")
        return client_ids

    def obtain_all_duplicate_catcher_messages(self, client_id):
        """
        Obtains all duplicate catcher messages from the connection log file.
        """
        file_path = f"{client_id}_{DUPLICATE_CATCHER_LOG_FILE_PATH}{self.suffix}"
        messages = []
        with self.lock:
            try:
                lines = read_file_bottom_to_top_generator(file_path)
                for line in lines:
                    message_id = line.strip()
                    messages.append(message_id)
            except FileNotFoundError:
                # The file doesn't exist
                logging.debug(
                    "The file doesn't exist, no duplicate catcher messages found"
                )
                return messages
        return messages

    def obtain_all_active_duplicate_catcher_clients(self):
        """
        Obtains all the active clients from all the duplicate catcher log files.

        It does this by reading the names of the files and extracting the client ids from them.
        """
        file_names = os.listdir()
        client_ids = []
        for file_name in file_names:
            if file_name.endswith(f"{DUPLICATE_CATCHER_LOG_FILE_PATH}{self.suffix}"):
                client_id = file_name.split("_")[0]
                client_ids.append(int(client_id))
        logging.debug(f"Active duplicate catcher clients: {client_ids}")
        return client_ids


def truncate_last_line_of_file(filename):
    """
    Truncates a file from the nth line from the bottom.

    Parameters:
        filename: The path of the file to truncate.
    """
    with open(filename, "rb+") as f:
        # Move the pointer (similar to a cursor in a text editor) to the end of the file
        f.seek(0, os.SEEK_END)

        # This code means the following code skips the very last character in the file -
        # i.e. in the case the last line is null we delete the last line
        # and the penultimate one
        pos = f.tell() - 1

        # Read each character in the file one at a time from the penultimate
        # character going backwards, searching for a newline character
        # If we find a new line, exit the search
        while pos > 0 and f.read(1) != b"\n":
            pos -= 1
            f.seek(pos, os.SEEK_SET)

        f.seek(pos, os.SEEK_SET)
        f.truncate()

        if pos > 0:
            # Write the end of file character
            f.write(b"\n")

        # Flush the file to disk
        f.flush()
        os.fsync(f.fileno())


def read_file_bottom_to_top_generator(filename, chunk_size=1024):
    """
    Generator that reads a file from the end to the beginning.

    Parameters:
        filename: The path of the file to read.
        chunk_size: The size of the chunks to read.
    """
    with open(filename, "rb") as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        remainder = b""

        while file_size > 0:
            read_size = min(chunk_size, file_size)
            f.seek(-read_size, os.SEEK_CUR)
            chunk = f.read(read_size)
            f.seek(-read_size, os.SEEK_CUR)
            file_size -= read_size

            lines = (chunk + remainder).decode("utf-8").splitlines()

            # Save the last line if it's incomplete
            remainder = lines[0].encode("utf-8")
            lines.pop(0)

            for line in reversed(lines):
                yield line

        # Yield the last line if it's incomplete
        if remainder:
            yield remainder.decode("utf-8")
