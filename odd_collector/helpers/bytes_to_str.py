from typing import Any, Dict, Optional

from funcy import walk_values


def convert_bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def convert_bytes_to_str_in_dict(values: Dict[str, Any]) -> Dict[str, Any]:
    return walk_values(convert_bytes_to_str, values)
