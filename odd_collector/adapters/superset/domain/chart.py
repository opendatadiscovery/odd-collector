from dataclasses import dataclass


@dataclass
class Chart:
    id: int
    slice_name: str
    dataset_id: int
    dashboard_ids: list[int]

    @classmethod
    def from_dict(cls, data: dict) -> "Chart":
        return cls(
            id=data["id"],
            slice_name=data["slice_name"],
            dataset_id=data["datasource_id"],
            dashboard_ids=[dashboard["id"] for dashboard in data["dashboards"]],
        )
