from typing import Any


def get_metadata(data: dict[str, Any], excluded_keys: list[str]) -> dict[str, Any]:
    return {key: data[key] for key in data if key not in excluded_keys}


def get_groups(data: dict[str, Any]) -> dict[str, Any]:
    transformed_data = data.copy()
    transformed_groups = {group["name"]: group for group in transformed_data["groups"]}
    transformed_data["groups"] = transformed_groups
    return transformed_data
