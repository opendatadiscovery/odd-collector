import copy
import re

from odd_collector.domain.rest_client.client import RequestArgs


class TestRestClient:
    def __init__(self):
        self.data_sources = {
            "_links": {"self": {"href": "/api/edserbinworkspace/data_sources"}},
            "_embedded": {
                "data_sources": [
                    {
                        "id": 40328,
                        "name": "mssql2_conn",
                        "description": "mssql2",
                        "token": "ecfa047adb2e",
                        "adapter": "jdbc:sqlserver",
                        "created_at": "2023-01-18T16:58:48.569Z",
                        "updated_at": "2023-01-30T10:20:39.054Z",
                        "has_expensive_schema_updates": False,
                        "public": False,
                        "asleep": False,
                        "queryable": True,
                        "soft_deleted": False,
                        "display_name": "mssql2_conn",
                        "account_id": 2685638,
                        "account_username": "edserbinworkspace",
                        "organization_token": "70615b238569",
                        "organization_plan_code": "free",
                        "database": "AdventureWorks",
                        "host": "test-host.compute.amazonaws.com",
                        "port": 1433,
                        "ssl": True,
                        "username": "SA",
                        "provider": "default",
                        "vendor": "sqlserver",
                        "ldap": False,
                        "warehouse": None,
                        "bridged": False,
                        "adapter_version": "9.2.1",
                        "custom_attributes": {"instance_name": ""},
                        "default_access_level": "Query",
                        "_links": {
                            "self": {
                                "href": "/api/edserbinworkspace/data_sources/ecfa047adb2e"
                            },
                            "creator": {"href": "/api/edserbin"},
                            "account": {"href": "/api/edserbinworkspace"},
                            "permissions": {
                                "href": "/api/edserbinworkspace/data_sources/ecfa047adb2e/permissions"
                            },
                            "web": {
                                "href": "/organizations/edserbinworkspace/data_sources/ecfa047adb2e"
                            },
                            "web_home": {
                                "href": "/home/edserbinworkspace/data_sources/ecfa047adb2e"
                            },
                        },
                        "_forms": {
                            "edit": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/data_sources/ecfa047adb2e",
                                "content_type": "application/json",
                                "input": {
                                    "data_source": {
                                        "tables[]": {
                                            "name": {"type": "text"},
                                            "description": {"type": "text"},
                                            "schema": {"type": "text"},
                                            "columns[]": {
                                                "name": {"type": "text"},
                                                "data_type": {"type": "text"},
                                                "is_nullable": {
                                                    "type": "select",
                                                    "options": [True, False],
                                                    "value": True,
                                                },
                                                "primary_key": {
                                                    "type": "select",
                                                    "options": [True, False],
                                                    "value": False,
                                                },
                                            },
                                        }
                                    }
                                },
                            }
                        },
                    },
                ]
            },
        }
        self.reports = {
            "_links": {
                "self": {
                    "href": "/api/edserbinworkspace/data_sources/ecfa047adb2e/reports"
                }
            },
            "_embedded": {
                "reports": [
                    {
                        "token": "a8a98afbd83a",
                        "id": 3899950,
                        "name": "Country Region Table",
                        "description": 'It is "Country Region Table"',
                        "created_at": "2023-01-18T17:02:36.824Z",
                        "updated_at": "2023-01-18T17:04:17.434Z",
                        "published_at": None,
                        "edited_at": "2023-01-18T17:03:31.268Z",
                        "theme_id": 3,
                        "color_mappings": {},
                        "type": "Report",
                        "last_successful_sync_at": None,
                        "last_saved_at": "2023-01-18T17:04:17.440Z",
                        "archived": False,
                        "space_token": "0b91fbe9fcaa",
                        "account_id": 2685638,
                        "account_username": "edserbinworkspace",
                        "public": False,
                        "full_width": False,
                        "manual_run_disabled": False,
                        "run_privately": True,
                        "drilldowns_enabled": True,
                        "layout": '<div class="mode-header embed-hidden">\n  <h1>{{ title }}</h1>\n  <p>{{ description }}</p>\n</div>\n<div class="mode-grid container">\n  <div class="row">\n    <div class="col-md-12">\n      <mode-chart id="chart_4dff8891a6fe" dataset="dataset" options="chart_options"></mode-chart>\n    </div>\n  </div>\n</div>',
                        "is_embedded": False,
                        "is_signed": False,
                        "shared": False,
                        "expected_runtime": 0.656525,
                        "last_successfully_run_at": "2023-01-18T17:02:38.119Z",
                        "last_run_at": "2023-01-18T17:02:37.224Z",
                        "web_preview_image": "https://s3.us-west-2.amazonaws.com/mode.production/report-run-previews/web/edserbinworkspace/a8a98afbd83a/d65e501a3279/571e6ae8f7ef?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAJZYSJ2XHDM2CQN2Q%2F20230130%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20230130T155108Z&X-Amz-Expires=119&X-Amz-SignedHeaders=host&X-Amz-Signature=a422d89c3d26536be73b4bf4883de5b6f5a6770142d73599018752ba7b7059ac",
                        "last_successful_run_token": "d65e501a3279",
                        "query_count": 1,
                        "max_query_count": 160,
                        "chart_count": 1,
                        "runs_count": 1,
                        "schedules_count": 0,
                        "query_preview": "SELECT * \nFROM Person.CountryRegion;",
                        "view_count": 5,
                        "_links": {
                            "self": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a"
                            },
                            "web": {
                                "href": "https://app.mode.com/edserbinworkspace/reports/a8a98afbd83a"
                            },
                            "web_edit": {
                                "href": "/editor/edserbinworkspace/reports/a8a98afbd83a"
                            },
                            "share": {
                                "href": "/edserbinworkspace/reports/a8a98afbd83a"
                            },
                            "web_report_runs": {
                                "href": "/edserbinworkspace/reports/a8a98afbd83a/runs"
                            },
                            "account": {"href": "/api/edserbinworkspace"},
                            "report_run": {
                                "templated": True,
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/runs/{id}?embed[result]=1",
                            },
                            "star": {"href": "/api/stars/9c50ed0cdd1b"},
                            "space": {
                                "href": "/api/edserbinworkspace/collections/0b91fbe9fcaa"
                            },
                            "queries": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/queries"
                            },
                            "report_runs": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/runs"
                            },
                            "report_pins": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/pins"
                            },
                            "report_schedules": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/schedules"
                            },
                            "python_visualizations": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/layout_cells"
                            },
                            "last_run": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/runs/d65e501a3279"
                            },
                            "last_successful_run": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/runs/d65e501a3279"
                            },
                            "perspective_email_subscription_memberships": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/perspective_email_report_subscription_memberships"
                            },
                            "validate_email_subscriber": {
                                "templated": True,
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/email_report_subscribers/validate{?subscriber[email]}",
                            },
                            "creator": {"href": "/api/edserbin"},
                            "report_theme": {
                                "href": "/api/modeanalytics/report_themes/3818837139b7"
                            },
                            "report_index_web": {
                                "href": "/edserbinworkspace/spaces/0b91fbe9fcaa/reports/a8a98afbd83a"
                            },
                            "dbt_metadata": {
                                "href": "/api/edserbinworkspace/reports/a8a98afbd83a/dbt_metadata"
                            },
                        },
                        "_forms": {
                            "edit": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/reports/a8a98afbd83a",
                                "input": {
                                    "report": {
                                        "name": {
                                            "type": "text",
                                            "value": "Country Region Table",
                                        },
                                        "description": {
                                            "type": "text",
                                            "value": 'It is "Country Region Table"',
                                        },
                                        "account_id": {
                                            "type": "text",
                                            "value": 2685638,
                                        },
                                        "space_token": {
                                            "type": "text",
                                            "value": "0b91fbe9fcaa",
                                        },
                                        "layout": {
                                            "type": "text",
                                            "value": '<div class="mode-header embed-hidden">\n  <h1>{{ title }}</h1>\n  <p>{{ description }}</p>\n</div>\n<div class="mode-grid container">\n  <div class="row">\n    <div class="col-md-12">\n      <mode-chart id="chart_4dff8891a6fe" dataset="dataset" options="chart_options"></mode-chart>\n    </div>\n  </div>\n</div>',
                                        },
                                        "color_mappings": {"type": "text", "value": {}},
                                        "published": {"type": "text", "value": False},
                                    }
                                },
                            },
                            "destroy": {
                                "method": "delete",
                                "action": "/api/edserbinworkspace/reports/a8a98afbd83a",
                            },
                            "archive": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/reports/a8a98afbd83a/archive",
                            },
                            "unarchive": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/reports/a8a98afbd83a/unarchive",
                            },
                            "batch_edit_filters": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/reports/a8a98afbd83a/filters/update_batch",
                                "input": {
                                    "report": {
                                        "filters[]": {
                                            "token": {"type": "text"},
                                            "options": {"type": "text"},
                                        }
                                    }
                                },
                            },
                            "clone": {
                                "method": "post",
                                "action": "/edserbinworkspace/reports/a8a98afbd83a/runs/d65e501a3279/clone",
                                "input": {
                                    "owner_id": {"type": "text", "value": 2685638}
                                },
                            },
                            "update_settings": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/reports/a8a98afbd83a/update_settings",
                                "input": {
                                    "report": {
                                        "theme": {
                                            "type": "select",
                                            "options": [
                                                ["Mode Dark", 1],
                                                ["Mode Grey", 2],
                                                ["Mode Light", 3],
                                                ["Mode Subway", 5],
                                            ],
                                            "value": 3,
                                        },
                                        "full_width": {
                                            "type": "select",
                                            "options": [True, False],
                                            "value": False,
                                        },
                                    }
                                },
                            },
                        },
                    }
                ]
            },
        }

        self.queries = {
            "_links": {
                "self": {"href": "/api/edserbinworkspace/reports/c714908743b2/queries"}
            },
            "_embedded": {
                "queries": [
                    {
                        "id": 14065955,
                        "token": "25c0cac7a889",
                        "raw_query": "SELECT *\nFROM Person.StateProvince;",
                        "created_at": "2023-01-18T17:14:04.087Z",
                        "updated_at": "2023-01-18T17:14:05.257Z",
                        "name": "Person.StateProvince",
                        "last_run_id": 2639738883,
                        "data_source_id": 40249,
                        "explorations_count": 0,
                        "report_imports_count": 0,
                        "dbt_metric_id": None,
                        "_links": {
                            "self": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889"
                            },
                            "report": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2"
                            },
                            "report_runs": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/runs"
                            },
                            "charts": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889/charts"
                            },
                            "new_chart": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889/charts/new"
                            },
                            "new_query_table": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889/tables/new"
                            },
                            "query_tables": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889/tables"
                            },
                            "query_runs": {
                                "href": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889/runs"
                            },
                            "creator": {"href": "/api/edserbin"},
                        },
                        "_forms": {
                            "edit": {
                                "method": "patch",
                                "action": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889",
                                "content_type": "application/json",
                                "input": {
                                    "query": {
                                        "raw_query": {
                                            "type": "text",
                                            "value": "SELECT *\nFROM Person.StateProvince;",
                                        },
                                        "name": {
                                            "type": "text",
                                            "value": "Person.StateProvince",
                                        },
                                        "data_source_id": {
                                            "type": "text",
                                            "value": 40249,
                                        },
                                    }
                                },
                            },
                            "destroy": {
                                "method": "delete",
                                "action": "/api/edserbinworkspace/reports/c714908743b2/queries/25c0cac7a889",
                            },
                        },
                    }
                ]
            },
        }

        self.collections = {
            "_links": {"self": {"href": "/api/edserbinworkspace/collections"}},
            "_embedded": {
                "spaces": [
                    {
                        "token": "553708880f11",
                        "id": 5851238,
                        "space_type": "community",
                        "name": "Community",
                        "description": None,
                        "state": "active",
                        "restricted": False,
                        "free_default": False,
                        "viewable?": True,
                        "default_access_level": "edit",
                        "_links": {
                            "self": {
                                "href": "/api/edserbinworkspace/collections/553708880f11"
                            },
                            "detail": {
                                "href": "/api/edserbinworkspace/collections/553708880f11/detail"
                            },
                            "web": {"href": "/edserbinworkspace/spaces/553708880f11"},
                            "reports": {
                                "href": "/api/edserbinworkspace/collections/553708880f11/reports"
                            },
                            "creator": {"href": "/api/edserbin"},
                            "search_space_permissions": {
                                "href": "/api/edserbinworkspace/collections/553708880f11/permissions/search"
                            },
                        },
                        "_forms": {
                            "destroy_entitlements": {
                                "method": "delete",
                                "action": "/api/edserbinworkspace/collections/553708880f11/permissions",
                            }
                        },
                    },
                    {
                        "token": "0b91fbe9fcaa",
                        "id": 5847598,
                        "space_type": "private",
                        "name": "Personal",
                        "description": None,
                        "state": "active",
                        "restricted": False,
                        "free_default": False,
                        "viewable?": True,
                        "default_access_level": "view",
                        "_links": {
                            "self": {
                                "href": "/api/edserbinworkspace/collections/0b91fbe9fcaa"
                            },
                            "detail": {
                                "href": "/api/edserbinworkspace/collections/0b91fbe9fcaa/detail"
                            },
                            "web": {"href": "/edserbinworkspace/spaces/0b91fbe9fcaa"},
                            "reports": {
                                "href": "/api/edserbinworkspace/collections/0b91fbe9fcaa/reports"
                            },
                            "creator": {"href": "/api/edserbin"},
                            "search_space_permissions": {
                                "href": "/api/edserbinworkspace/collections/0b91fbe9fcaa/permissions/search"
                            },
                        },
                    },
                    {
                        "token": "a6893bb5ad83",
                        "id": 5847599,
                        "space_type": "custom",
                        "name": "edserbinworkspace",
                        "description": None,
                        "state": "active",
                        "restricted": False,
                        "free_default": True,
                        "viewable?": True,
                        "viewed?": True,
                        "default_access_level": "edit",
                        "_links": {
                            "self": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83"
                            },
                            "detail": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/detail"
                            },
                            "web": {"href": "/edserbinworkspace/spaces/a6893bb5ad83"},
                            "reports": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/reports"
                            },
                            "creator": {"href": "/api/edserbin"},
                            "user_space_membership": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/memberships/d2a5dc6486f2"
                            },
                            "space_memberships": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/memberships"
                            },
                            "preview_space_memberships": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/memberships/preview"
                            },
                            "search_space_permissions": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/permissions/search"
                            },
                            "viewed": {
                                "href": "/api/edserbinworkspace/collections/a6893bb5ad83/viewed"
                            },
                        },
                        "_forms": {
                            "destroy_entitlements": {
                                "method": "delete",
                                "action": "/api/edserbinworkspace/collections/a6893bb5ad83/permissions",
                            }
                        },
                    },
                ]
            },
        }

    async def fetch(self, session, request_args: RequestArgs):
        response = None
        if "/api/account/data_sources" in request_args.url:
            response = self.data_sources
        if "/api/account/spaces" in request_args.url:
            response = self.collections
        if re.match(
            r".*/api/account/data_sources/[a-zA-Z\d_.-]*/reports", request_args.url
        ) or re.match(
            r".*/api/account/spaces/[a-zA-Z\d_.-]*/reports", request_args.url
        ):
            response = self.reports
        if re.match(r".*/api/account/reports/[a-zA-Z\d_.-]*/queries", request_args.url):
            response = self.queries
        return response
