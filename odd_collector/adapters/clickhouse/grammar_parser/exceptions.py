class ParserError(Exception):
    pass


class NonTypeObjectError(ParserError):
    pass


class UnexpectedTypeError(ParserError):
    pass


class StructureError(ParserError):
    pass
