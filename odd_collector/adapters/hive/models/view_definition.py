from dataclasses import dataclass


@dataclass
class ViewDefinition:
    original_text: str = None
    expanded_text: str = None
    rewrite_enabled: str = None
