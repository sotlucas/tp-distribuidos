import logging
from commons.logger import Logger, RestoreType


class RestoreState:
    def __init__(
        self, messages_received, messages_sent, possible_duplicates, duplicate_catchers
    ):
        self.messages_received = messages_received
        self.messages_sent = messages_sent
        self.possible_duplicates = possible_duplicates
        self.duplicate_catchers = duplicate_catchers


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
                "duplicate_catchers": {},
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
        state["duplicate_catchers"] = {
            int(k): v for k, v in state.get("duplicate_catchers", {}).items()
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
            state.get("duplicate_catchers", {}),
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

    def get_duplicate_catchers(self):
        logging.info(
            "restoring duplicate_catchers: {}".format(
                self.restored_state.duplicate_catchers
            )
        )
        return self.restored_state.duplicate_catchers.copy()

        """
        state:{
            "messages_received": {
                "1": 10,
            },
            "possible_duplicates": {"1": [1, 2, 3]},
            "messages_sent": {
                "1": 0,
            }
            "duplicate_catcher": {
                "1": [1, 2, 3, 4, 5, 6, 7, 8, 9]
            }
        }
        """


# # Log Communication
# START 83
# SENT 83
# SAVE BEGIN 83
# {
#     "messages_received": {
#         "1": 10,
#     },
#     "possible_duplicates": {"1": [1, 2, 3]},
#     "messages_sent": {
#         "1": 0,
#     },
#     "duplicate_catcher": {
#         "1": [1, 2, 3, 4, 5, 6, 7, 8, 9],
#         "2": [1, 2, 3, 4, 5, 6, 7, 8, 9],
#     }
# }
# SAVE DONE 83
# COMMIT 83


# # Log Connection {client_1}
# 81/"routes":{"EZE-MIA": 321}
# 82/"routes":{"EZE-MIA": 89898}
# 83/"routes":{"ASO-KLO": 666}
# EOF/"vuelos_message_to_send":[...]


# # SAVE DONE en Log Communication despues de guardar en el Log de Connection
