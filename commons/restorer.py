from commons.communication import Communication
from commons.logger import Logger, RestoreType

class RestoreState:
    def __init__(self, messages_received, messages_sent, possible_duplicates, duplicate_catcher):
        self.messages_received = messages_received
        self.messages_sent = messages_sent
        self.possible_duplicates = possible_duplicates
        self.duplicate_catcher = duplicate_catcher

    def get_messages_received(self):
        return self.messages_received
    
    def get_messages_sent(self):
        return self.messages_sent

    def get_possible_duplicates(self):
        return self.possible_duplicates
    
    def get_duplicate_catcher(self):
        return self.duplicate_catcher


class Restorer:
    """
    The Restorer fetches the last state of the processors from the log file and restores them.
    """

    def __init__(self):
        self.logger = Logger(LOG_FILE_PATH)

    def restore(self):
        """
        Restore the state of the processors from the log file.
        """
        restore_type, message_id, client_id, state = self.logger.restore()
        if restore_type == RestoreType.SAVE_DONE or restore_type == RestoreType.SENT:
            state["possible_duplicates"][client_id].append(message_id)
        return RestoreState(
            state.get("messages_received", {}),
            state.get("messages_sent", {}),
            state.get("possible_duplicates", {}),
            state.get("duplicate_catcher", {}),
        )

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




# Log Communication
START 83
SENT 83
SAVE BEGIN 83
{
    "messages_received": {
        "1": 10,
    },
    "possible_duplicates": {"1": [1, 2, 3]},
    "messages_sent": {
        "1": 0,
    },
    "duplicate_catcher": {
        "1": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "2": [1, 2, 3, 4, 5, 6, 7, 8, 9],
    }
}
SAVE DONE 83
COMMIT 83


# Log Connection {client_1}
81/"routes":{"EZE-MIA": 321}
82/"routes":{"EZE-MIA": 89898}
83/"routes":{"ASO-KLO": 666}
EOF/"vuelos_message_to_send":[...]

