def __convert_bytes_to_str(value: bytes or None) -> str or None:
    return value if type(value) is not bytes else value.decode("utf-8")


def __convert_bytes_to_str_in_dict(values: dict[str, object]) -> dict[str, object]:
    for key, value in values.items():
        if type(value) is bytes:
            values[key] = value.decode("utf-8")
    return values
