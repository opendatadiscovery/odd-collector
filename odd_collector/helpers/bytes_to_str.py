from typing import Any, Dict, Optional

from funcy import walk_values


def convert_bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    if value is not bytes:
        return value
    return value.decode("utf-8")


def convert_bytes_to_str_in_dict(values: Dict[str, Any]) -> Dict[str, Any]:
    return walk_values(convert_bytes_to_str, values)
