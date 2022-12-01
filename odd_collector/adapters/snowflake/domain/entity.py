from typing import List, Optional, Union

from pydantic import BaseModel, validator

from ..logger import logger


class Connection(BaseModel):
    table_catalog: str
    table_name: str
    table_schema: str
    domain: str  # TABLE | VIEW


class Connectable(BaseModel):
    upstream: Optional[List[Connection]] = []
    downstream: Optional[List[Connection]] = []

    @validator("upstream", "downstream", pre=True)
    def split_str(cls, nodes: Union[str, List[Connection]]):
        """
        For cases when upstream or downstream are string like 'database.schema.name'
        """
        if not nodes:
            return []

        if isinstance(nodes, str):
            result = []
            try:
                for node in nodes.split(","):
                    catalog, schema, name, domain = node.split(".")
                    result.append(
                        Connection(
                            table_catalog=catalog,
                            table_schema=schema,
                            table_name=name,
                            domain=domain,
                        )
                    )
                return result
            except Exception:
                logger.error(
                    f"Couldn't parse nodes string. Wait string format 'db.schema.table.(TABLE|VIEW)', got: {nodes}"
                )
                return []
        return nodes
