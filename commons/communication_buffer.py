import multiprocessing as mp
import socket

from commons.protocol import (
    EOFMessage,
    Message,
    MessageProtocolType,
)

BUFFER_SIZE = 8192  # 8 KiB
END_OF_MESSAGE = b"\r\n\r\n"


class CommunicationBuffer:
    """
    Communication buffer for a socket.
    """

    def __init__(self, sock, timeout=None):
        self.sock = sock
        if timeout:
            self.sock.settimeout(timeout)
        else:
            self.sock.setblocking(True)
        self.buffer = b""
        self.lock = mp.Lock()

    def get_message(self):
        """
        Get a message from the socket.
        """
        while END_OF_MESSAGE not in self.buffer:
            data = self.sock.recv(BUFFER_SIZE)
            if not data:  # socket is closed
                raise PeerDisconnected
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(END_OF_MESSAGE)
        return Message.from_bytes(line)

    def send_message(self, message: Message):
        """
        Send a message through the socket.
        """
        with self.lock:
            self.sock.sendall(message.to_bytes() + END_OF_MESSAGE)

    def send_eof(self, eof_type: MessageProtocolType):
        """
        Send an EOF message through the socket.
        """
        message = EOFMessage(eof_type)
        self.send_message(message)

    def stop(self):
        """
        Graceful shutdown. Closing all connections.
        """
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()


class PeerDisconnected(Exception):
    pass
