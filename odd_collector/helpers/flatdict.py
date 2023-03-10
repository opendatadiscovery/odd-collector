import flatdict


class FlatDict(flatdict.FlatDict):
    def __init__(self, value, delimiter=":", **kwargs):
        super().__init__(value=value, delimiter=delimiter, **kwargs)
