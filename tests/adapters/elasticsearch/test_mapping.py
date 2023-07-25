from unittest import TestCase

from odd_models.models import DataEntity, Type

from odd_collector.adapters.elasticsearch.adapter import Adapter, ElasticSearchGenerator
from odd_collector.adapters.elasticsearch.mappers.stream import (
    map_data_stream,
    map_data_stream_template,
)
from odd_collector.domain.plugin import ElasticsearchPlugin


class TestDataStreamMapping(TestCase):
    def setUp(self) -> None:
        self.oddrn_generator = ElasticSearchGenerator(host_settings="test")
        config = ElasticsearchPlugin(
            host="localhost", port="9200", type="elasticsearch", name="test_elastic"
        )
        self.adapter = Adapter(config)

    def test_stream_mapping(self):
        data_stream = {
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
        }

        tempalte_metadata = [
            {
                "name": "my_template",
                "index_template": {
                    "index_patterns": ["mystream*"],
                    "template": {
                        "settings": {
                            "index": {
                                "number_of_shards": "1",
                                "number_of_replicas": "1",
                            }
                        },
                        "mappings": {
                            "properties": {
                                "@timestamp": {"type": "date"},
                                "name": {"type": "text"},
                                "appointment": {"type": "date"},
                                "type": {"type": "text"},
                                "userid": {"type": "long"},
                            }
                        },
                    },
                    "composed_of": [],
                    "data_stream": {},
                },
            }
        ]

        data_entity = map_data_stream(
            data_stream, tempalte_metadata, None, self.oddrn_generator
        )

        self.assertIsNotNone(data_entity)

        self.assertEqual(
            data_entity.oddrn, "//elasticsearch/host/test/indexes/mystream"
        )
        self.assertIsInstance(data_entity, DataEntity)
        self.assertEqual(len(data_entity.dataset.field_list), 4)

        fields = data_entity.dataset.field_list

        self.assertEqual(fields[0].name, "name")
        self.assertEqual(fields[0].type.type, Type.TYPE_STRING)

        self.assertEqual(fields[1].name, "appointment")
        self.assertEqual(fields[1].type.type, Type.TYPE_DATETIME)

        self.assertEqual(fields[2].name, "type")
        self.assertEqual(fields[2].type.type, Type.TYPE_STRING)

        self.assertEqual(fields[3].name, "userid")
        self.assertEqual(fields[3].type.type, Type.TYPE_INTEGER)

    def test_map_data_stream_template(self):
        tempalte_metadata = [
            {
                "name": "my_template",
                "index_template": {
                    "index_patterns": ["mystream*"],
                    "template": {
                        "settings": {
                            "index": {
                                "number_of_shards": "1",
                                "number_of_replicas": "1",
                            }
                        },
                        "mappings": {
                            "properties": {
                                "@timestamp": {"type": "date"},
                                "name": {"type": "text"},
                                "appointment": {"type": "date"},
                                "type": {"type": "text"},
                                "userid": {"type": "long"},
                            }
                        },
                    },
                    "composed_of": [],
                    "data_stream": {},
                },
            }
        ]

        data_stream = {
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
        }

        data_stream_entity = map_data_stream(
            data_stream, tempalte_metadata, None, self.oddrn_generator
        )

        data_stream_oddrn = [data_stream_entity.oddrn]
        data_entity = map_data_stream_template(
            tempalte_metadata, data_stream_oddrn, self.oddrn_generator
        )

        self.assertIsInstance(data_entity, DataEntity)
        self.assertEqual(len(data_entity.dataset.field_list), 4)

        fields = data_entity.dataset.field_list

        self.assertEqual(fields[0].name, "name")
        self.assertEqual(fields[0].type.type, Type.TYPE_STRING)

        self.assertEqual(fields[1].name, "appointment")
        self.assertEqual(fields[1].type.type, Type.TYPE_DATETIME)

        self.assertEqual(fields[2].name, "type")
        self.assertEqual(fields[2].type.type, Type.TYPE_STRING)

        self.assertEqual(fields[3].name, "userid")
        self.assertEqual(fields[3].type.type, Type.TYPE_INTEGER)

        self.assertListEqual(
            data_entity.data_entity_group.entities_list, data_stream_oddrn
        )
