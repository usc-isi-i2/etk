class GlossaryExtractor(Extractor):
    def __init__(self, glossary, ngrams=2, case_sensitive=False):
        self.glossary = glossary
        self.ngrams = ngrams
        self.case_sensitive = case_sensitive

    @property
    def input_type(self):
        """
        The type of input that an extractor wants
        Returns: InputType
        """
        return self.InputType.TEXT

    # @property
    def name(self):
        if self.glossary.name is None:
            return "unknown glossary extractor"
        return self.glossary.name + " extractor"

    # @property
    def category(self):
        return "glossary"

    def extract(self, extractables):