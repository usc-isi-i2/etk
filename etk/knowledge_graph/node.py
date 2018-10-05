class Node(object):

    def __eq__(self, other):
        raise NotImplementedError

    def __hash__(self):
        raise NotImplementedError

    @property
    def value(self):
        raise NotImplementedError

    def is_valid(self):
        raise NotImplementedError


class URI(Node):
    def __init__(self, value):
        pass


class BNode(Node):
    def __init__(self, value):
        pass


class Literal(Node):
    def __init__(self, value, lang=None, type_=None):
        pass