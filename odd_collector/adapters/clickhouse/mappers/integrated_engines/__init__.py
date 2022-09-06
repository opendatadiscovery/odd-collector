from typing import Dict, List, Optional, Type

from ...domain import IntegrationEngine
from .engine_parsers import BaseEngineParser, KafkaEngineParser

ENGINE_PARSER_MAP: Dict[str, Type[BaseEngineParser]] = {
    "Kafka": KafkaEngineParser,
}


def parse_from_intergated_engine(
    source_table: str, intgr_engine_dict: Dict[str, IntegrationEngine]
) -> Optional[List[str]]:
    engine = intgr_engine_dict.get(source_table)
    if not engine:
        return None
    engine_parser_cls = ENGINE_PARSER_MAP.get(engine.name)
    if not engine_parser_cls:
        return None
    engine_parser_obj = engine_parser_cls(source_table, engine.settings)
    return engine_parser_obj.get_oddrns()
