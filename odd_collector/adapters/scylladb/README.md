# ScyllaDB Adapter

ScyllaDB is a highly performant and scalable NoSQL database compatible with Apache Cassandra. 
However, there are incompatibilities in Cassandra and ScyllaDB schemas: 
- **cdc** Scylla expects a dictionary such as cdc = {'enabled': 'false'}, unlike Cassandra accepts bool.
- **additional_write_policy** = '99p' Scylla doesn't support this.
- **extensions** = {} fails with SyntaxException: Unknown property 'extensions'
- **read_repair** = 'BLOCKING' fails with SyntaxException: Unknown property 'read_repair'
- **ConfigurationException**: Missing sub-option 'sstable_compression' for the 'compression' option and
- **compression** = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} causes ConfigurationException: Unknown compression option 'class'.

In this regard we adapt ScyllaDB [models](./mappers/models.py) in order to parse data correctly. Other than that, ScyllaDB collector logic is similar to Cassandra.
More info on incompatibilities can be found [here](https://github.com/scylladb/scylladb/issues/9859).
