from collections import UserDict


class LowerKeyDict(UserDict):
    """
    Helper class which transform keys to lower case.

    >>>> d = LowerKeyDict({'FIELD': 1})
    >>>> assert 'a' in d
    """

    def __init__(self, data):
        super().__init__(data)

    def __setitem__(self, key: str, item):
        self.data[key.lower()] = item
