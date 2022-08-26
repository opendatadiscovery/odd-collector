from typing import Any, List


# TODO!: Add drill_members
# TODO!: Add drill_members_grouped
class Measure:
    def __init__(
        self,
        name: str,
        title: str,
        short_title: str,
        cumulative_total: bool,
        cumulative: bool,
        measure_type: str,
        agg_type: str,
        is_visible: bool,
        drill_members: List[Any],
        drill_members_grouped: List[Any],
    ):
        self.name = name
        self.title = title
        self.short_title = short_title
        self.cumulative_total = cumulative_total
        self.cumulative = cumulative
        self.measure_type = measure_type
        self.agg_type = agg_type
        self.is_visible = is_visible
        self.drill_members = drill_members
        self.drill_members_grouped = drill_members_grouped

    @staticmethod
    def from_response(response):
        return Measure(
            name=response.get("name"),
            title=response.get("title"),
            short_title=response.get("shortTitle"),
            cumulative_total=response.get("cumulativeTotal"),
            cumulative=response.get("cumulative"),
            measure_type=response.get("type"),
            agg_type=response.get("aggType"),
            is_visible=response.get("isVisible"),
            drill_members=response.get("drillMembers"),
            drill_members_grouped=response.get("drillMembersGrouped"),
        )
