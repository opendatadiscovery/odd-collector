from typing import Dict, Type, List, Optional

from .engine_parsers import BaseEngineParser, KafkaEngineParser
from .. import IntgrEngineNamedtuple

ENGINE_PARSER_MAP: Dict[str, Type[BaseEngineParser]] = {
    "Kafka": KafkaEngineParser,
}


def parse_from_intergated_engine(source_table: str,
                                 intgr_engine_dict: Dict[str, IntgrEngineNamedtuple]) -> Optional[List[str]]:
    engine = intgr_engine_dict.get(source_table)
    if not engine:
        return
    engine_parser_cls = ENGINE_PARSER_MAP.get(engine.name)
    if not engine_parser_cls:
        return
    engine_parser_obj = engine_parser_cls(source_table, engine.settings)
    return engine_parser_obj.get_oddrns()
