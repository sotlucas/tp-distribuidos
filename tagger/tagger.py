class Tagger:
    def __init__(self, tag_name, receiver, sender):
        self.tag_name = tag_name
        self.receiver = receiver
        self.sender = sender

    def run(self):
        self.receiver.bind(
            input_callback=self.tag_message, eof_callback=self.sender.send_eof
        )
        self.receiver.start()

    def tag_message(self, message):
        """
        Adds the tag name to the beginning of the message and sends it to the output.
        """
        self.sender.send(f"[{self.tag_name}]{message}")
