import unittest

from odd_collector.adapters.elasticsearch.adapter import Adapter, ElasticSearchGenerator
from odd_collector.domain.plugin import ElasticsearchPlugin


class TestParserDataStream(unittest.TestCase):
    def setUp(self) -> None:
        self.oddrn_generator = ElasticSearchGenerator(host_settings="test")
        config = ElasticsearchPlugin(
            host="localhost", port="9200", type="elasticsearch", name="test_elastic"
        )
        self.adapter = Adapter(config)

    def test_parse_template_from_data_stream(self):
        data_streams = [
            {
                "name": "mystream",
                "timestamp_field": {"name": "@timestamp"},
                "indices": [
                    {
                        "index_name": ".ds-mystream-000001",
                        "index_uuid": "yQwwZE7-QbS3BqxbhKyXww",
                    }
                ],
                "generation": 1,
                "status": "YELLOW",
                "template": "my_template",
            },
            {
                "name": "mystreamanother",
                "timestamp_field": {"name": "@timestamp"},
                "indices": [
                    {
                        "index_name": ".ds-mystreamanother-000001",
                        "index_uuid": "yQwwZE7-QbS3BqxbhkefXww",
                    }
                ],
                "generation": 1,
                "status": "YELLOW",
                "template": "my_template",
            },
            {
                "name": "my_data_stream",
                "timestamp_field": {"name": "@timestamp"},
                "indices": [
                    {
                        "index_name": ".ds-my_data_stream-000001",
                        "index_uuid": "yQwwZE7-QbS3BqxbhkefXww",
                    }
                ],
                "generation": 1,
                "status": "YELLOW",
                "template": "another_template",
            },
        ]

        expected_result = {
            "my_template": [
                {
                    "name": "mystream",
                    "timestamp_field": {"name": "@timestamp"},
                    "indices": [
                        {
                            "index_name": ".ds-mystream-000001",
                            "index_uuid": "yQwwZE7-QbS3BqxbhKyXww",
                        }
                    ],
                    "generation": 1,
                    "status": "YELLOW",
                    "template": "my_template",
                },
                {
                    "name": "mystreamanother",
                    "timestamp_field": {"name": "@timestamp"},
                    "indices": [
                        {
                            "index_name": ".ds-mystreamanother-000001",
                            "index_uuid": "yQwwZE7-QbS3BqxbhkefXww",
                        }
                    ],
                    "generation": 1,
                    "status": "YELLOW",
                    "template": "my_template",
                },
            ],
            "another_template": [
                {
                    "name": "my_data_stream",
                    "timestamp_field": {"name": "@timestamp"},
                    "indices": [
                        {
                            "index_name": ".ds-my_data_stream-000001",
                            "index_uuid": "yQwwZE7-QbS3BqxbhkefXww",
                        }
                    ],
                    "generation": 1,
                    "status": "YELLOW",
                    "template": "another_template",
                },
            ],
        }

        template_mappings = self.adapter.get_templates_from_data_streams(data_streams)

        self.assertDictEqual(template_mappings, expected_result)
