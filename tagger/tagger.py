class Tagger:
    def __init__(self, tag_name) -> None:
        self.tag_name = tag_name

    def tag_message(self, message):
        """
        Adds the tag name to the beginning of the message.
        """
        return f"[{self.tag_name}]{message}"
