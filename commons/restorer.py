import logging
from commons.logger import Logger, RestoreType


class RestoreState:
    def __init__(self, messages_received, messages_sent, possible_duplicates):
        self.messages_received = messages_received
        self.messages_sent = messages_sent
        self.possible_duplicates = possible_duplicates


class Restorer:
    """
    The Restorer fetches the last state of the processors from the log file and restores them.
    """

    def __init__(self, suffix=""):
        logger = Logger(suffix)
        self.restored_state = Restorer.restore(logger)

    def restore(logger):
        """
        Restore the state of the processors from the log file.
        """
        restore_type, message_id, client_id, state = logger.restore()
        if not state:
            # The log file has not state
            state = {
                "messages_received": {},
                "messages_sent": {},
                "possible_duplicates": {},
            }

        # convert the keys to int
        state["messages_received"] = {
            int(k): v for k, v in state.get("messages_received", {}).items()
        }
        state["messages_sent"] = {
            int(k): v for k, v in state.get("messages_sent", {}).items()
        }
        state["possible_duplicates"] = {
            int(k): v for k, v in state.get("possible_duplicates", {}).items()
        }

        if restore_type == RestoreType.SAVE_DONE or restore_type == RestoreType.SENT:
            logging.debug(
                f"Restorer: Message: {message_id} did not finish correctly, adding it to the possible duplicates"
            )
            state["possible_duplicates"][client_id] = state["possible_duplicates"].get(
                client_id, []
            ) + [message_id]

        return RestoreState(
            state.get("messages_received", {}),
            state.get("messages_sent", {}),
            state.get("possible_duplicates", {}),
        )

    def get_messages_received(self):
        logging.info(
            "restoring messages_received: {}".format(
                self.restored_state.messages_received
            )
        )
        return self.restored_state.messages_received.copy()

    def get_messages_sent(self):
        logging.info(
            "restoring messages_sent: {}".format(self.restored_state.messages_sent)
        )
        return self.restored_state.messages_sent.copy()

    def get_possible_duplicates(self):
        logging.info(
            "restoring possible_duplicates: {}".format(
                self.restored_state.possible_duplicates
            )
        )
        return self.restored_state.possible_duplicates.copy()
