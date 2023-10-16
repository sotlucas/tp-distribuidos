class Tagger:
    def __init__(self, tag_name, receiver, sender):
        self.tag_name = tag_name
        self.receiver = receiver
        self.sender = sender

    def run(self):
        self.receiver.bind(
            input_callback=self.tag_messages, eof_callback=self.sender.send_eof
        )
        self.receiver.start()

    def tag_messages(self, messages):
        tagged_messages = [self.tag_message(message) for message in messages]
        self.sender.send_all(tagged_messages)

    def tag_message(self, message):
        """
        Adds the tag name to the beginning of the message and sends it to the output.
        """
        return f"[{self.tag_name}]{message}"
