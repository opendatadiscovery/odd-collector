from odd_models.models import MetadataExtension
from ..domain.report import Report


SCHEMA_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/mode.json#/definitions/Mode"
)


def map_report_metadata(report: Report) -> MetadataExtension:
    report_fields = vars(report).copy()
    already_used_fields = ["name", "updated_at", "created_at"]
    unnecessary_fields = ["links", "query_preview"]
    for field in already_used_fields + unnecessary_fields:
        report_fields.pop(field)

    report_fields["queries"] = (
        ";\n\n".join([r.raw_query.strip("; \n") for r in report.queries]) + ";"
    )

    return MetadataExtension(schema_url=SCHEMA_URL, metadata=report_fields)
