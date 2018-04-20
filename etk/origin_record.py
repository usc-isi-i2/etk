

class OriginRecord():
    """
    An individual Origin Record of an extraction result from a document.
    """
    def __init__(self, json_path: str, start_char: str, end_char:str, document=None) -> None:
        # Extractable.__init__(self)
        self.json_path = json_path
        self.start_char = start_char
        self.end_char = end_char
        self._document = document

    @property
    def full_path(self) -> str:
        """
        Returns: The full path of a JSONPath match
        """
        return self.json_path

    @property
    def document(self):
        """
        Returns: the parent Document. It's prese
        """
        return self._document
