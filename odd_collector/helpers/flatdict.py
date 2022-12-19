import flatdict


class FlatDict(flatdict.FlatDict):
    def __init__(self, value):
        super().__init__(value, delimiter=",")
