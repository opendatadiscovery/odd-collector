from typing import Optional

from odd_models.models import DataConsumer, DataEntity, DataEntityType
from oddrn_generator import Generator, PostgresqlGenerator

from ..domain import Card, Table
from .metadata import get_metadata


def generate_table_oddrn(table: Table):
    database = table.db
    if database.engine == "postgres":
        details = database.details

        host = details.host if details.host != "host.docker.internal" else "localhost"

        generator = PostgresqlGenerator(
            host_settings=host,
            databases=details.dbname,
            schemas=table.schem,
            tables=table.name,
        )
        return generator.get_oddrn_by_path("tables")
    return None


def map_card(card: Card, table: Optional[Table], generator: Generator):
    generator.set_oddrn_paths(collections=card.collection_id or "root", cards=card.id)
    inputs = []
    if table:
        inputs.append(generate_table_oddrn(table))

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("cards"),
        name=card.name,
        description=card.description,
        owner=card.get_owner(),
        metadata=get_metadata(card),
        tags=None,
        updated_at=card.updated_at,
        created_at=card.created_at,
        type=DataEntityType.FILE,  # TODO: what is type for charts/cards
        data_consumer=DataConsumer(inputs=inputs),
    )
