### Views Concept in Couchbase
Views are deprecated in Couchbase Server 7.0+. Views support in Couchbase Server will be removed in a future release only when the core functionality of the View engine is covered by other services.
##### Note: 
As of now we are unable to create view indexes due to Couchbase SDK limitations.


### Primary Index
Couchbase doesn't support column as a primary key, it has primary index insted.
The primary index is simply an index on the document key on the entire keyspace. 


### Usage of INFER statement and `num_sample_values`/`sample_size` parameters
The INFER statement enables you to infer the metadata of documents in a keyspace, for example the structure of documents, 
data types of various attributes, sample values, and so on. Since a keyspace can contain documents with varying structures, 
the INFER statement is statistical in nature rather than deterministic. 


`sample_size`
Specifies the number of documents to randomly sample in the keyspace when inferring the schema. The default sample size is 10 documents.

`num_sample_values`
Specifies the number of sample values for each attributes to be returned. The sample values provide examples of the data format. The default value is 0.
##### Note: 
In Couchbase, all numbers are stored as a JSON number type, which means they are represented as a string in the JSON document but stored as a binary value in the database.
In this case set `num_sample_values` to 1 so we can get specific `int/float` type of its value manually.
